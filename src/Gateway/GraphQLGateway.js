/**
 * @file createGraphQLGateway
 * @author Brad Decker <brad.decker@conciergeauctions.com>
 *
 * The primary purpose of this file is to build a schema that stitches
 * together the schemas of all remote services that have been defined
 * using the graphql mixin also provided by this library.
 * @flow
 */
import fs from 'fs';
import selectn from 'selectn';
import isEmpty from 'lodash.isempty';
import diffBy from 'lodash.differenceby';
import difference from 'lodash.difference';
import { mergeSchemas } from 'graphql-tools';
import type { GraphQLSchema, DocumentNode } from 'graphql';
import type { ServiceBroker, ServiceWorker } from 'moleculer';
import { printSchema, parse, graphql as execute, buildSchema } from 'graphql';

/* Import from source-code */
import { getRelatedTypes } from './utilities';
import { createRemoteSchema } from './createRemoteSchemaV2';
import { buildRelationalResolvers } from './buildRelationalResolvers';
import type { TypeRelationDefinitions } from '../Types/ServiceConfiguration';

import type { ExtendGqlSchema } from './createRemoteSchemaV2';
opaque type ServiceName = string;

type GatewayOptions = {
  broker: ServiceBroker,
  waitTimeout?: number,
  waitInterval?: number,
  blacklist?: Array<string>,
  generateSnapshot?: boolean,
  snapshotPath?: string,
};

type RemoteSchemaMap = {
  [TypeName: string]: ExtendGqlSchema,
};

type RelationshipSchemas = {
  [TypeName: string]: string,
};

type GraphQLTypeServiceMap = {
  [type: GraphQLTypeName]: ServiceName,
};

type HashMapRegisteredService = {
  [key: string]: boolean,
};

export class GraphQLGateway {
  // Services to ignore
  blacklist: Array<string> = ['$node'];
  // Passed in service broker used to make calls
  broker: ServiceBroker;
  // Running list of discovered types and the service that they belong to
  discoveredTypes: GraphQLTypeServiceMap = {};
  // Define absolutely necessary types before schema can be complete
  // If true, save a snapshot schema file everytime the schema changes
  generateSnapshot: boolean = false;
  // Boolean to track whether the schema has been initialized
  initialized: boolean = false;
  // Method to hook into service discovery.
  relationDefinitions: TypeRelationDefinitions = {};
  // Additional Schemas for relating objects across services
  relationships: RelationshipSchemas = {};
  // Remove Schema map for storing the remote schemas created
  remoteSchemas: RemoteSchemaMap = {};
  // The current schema for the gateway, computed by stitching remote schemas
  schema: ?GraphQLSchema = null;
  // Internal service for listening for events
  service: ?ServiceWorker = null;
  // Path to save the snapshot to
  snapshotPath: string = `${process.cwd()}/schema.snapshot.graphql`;
  // Keep track on which service registered

  logEvent = (eventName: string) => (func: any) => (...args: any[]) => {
    this.service.logger.info('[moleculer-graphql] Event', eventName);
    return func(...args);
  };

  getGqlServices = (services: Array<any>): Array<any> =>
    services.filter(service => {
      const notInBlackList = !this.blacklist.includes(service.name);
      const hasSchema = service.settings.schemaStr;

      return hasSchema && notInBlackList;
    });

  addGql = async (services: Array<any>): Promise<void> => {
    const gqlServices = this.getGqlServices(services);
    this.service.logger.info('[moleculer-graphql] Add GQL. Services:', gqlServices.map(s => s.name));

    gqlServices.map(service => this.buildRemoteSchema(service));
    this.generateSchema();
  };

  removeGql = async (services: any): Promise<void> => {
    const currServices = await this.broker.call('$node.services');
    const gqlServices = this.getGqlServices(services);
    const currGqlServices = this.getGqlServices(currServices);
    this.service.logger.info('[moleculer-graphql] Remove GQL. Services:', gqlServices.map(s => s.name));

    //
    const beRmServices = diffBy(services, currGqlServices, service => service.name);
    beRmServices.map(service => this.removeRemoteSchema(service));

    this.generateSchema();
  };

  handleGqlAdded = (service: any): void => {
    this.addGql([service]);
  };

  handleGqlRemoved = (service: any): void => {
    this.removeGql([service]);
  };

  handleServiceUpdate = (): Promise<void> => {
    // * In moleculer 0.13.5, event "$services.changed" only means service loaded by broker
    // * It doesn't means:
    // *   - Service started, here is only created
    // *   - Payload only indicate service is "local" or not
    return this.broker.call('$node.services').then(this.addGql);
  };

  scanAllServices = (): Promise<void> => {
    return this.broker.call('$node.services').then(this.addGql);
  };

  handleNodeConnection = ({ node }: Object): Promise<void> => {
    if (!node) {
      console.log('[moleculer-graphql][handleNodeConnection] node: empty');
      return Promise.resolve();
    }

    return this.addGql(node.services);
  };

  handleNodeDisconnected = async ({ node }: Object): Promise<void> => {
    if (!node) {
      console.log('[moleculer-graphql][handleNodeDisconnected] node: empty');
      return Promise.resolve();
    }

    this.removeGql(node.services);
  };

  constructor(opts: GatewayOptions) {
    this.broker = opts.broker;
    if (opts.blacklist) this.blacklist.concat(opts.blacklist);
    if (opts.generateSnapshot) this.generateSnapshot = opts.generateSnapshot;
    if (opts.snapshotPath) this.snapshotPath = opts.snapshotPath;

    this.service = this.broker.createService({
      name: '@gateway',
      events: {
        '$gql.added': this.logEvent('$gql.added')(this.handleGqlAdded),
        '$gql.removed': this.logEvent('$gql.removed')(this.handleGqlRemoved),

        // * Event "$services.changed" thrown when broker load service
        // * We can't trust in this hook to merge schema
        // '$services.changed': this.handleServiceUpdate,
        '$broker.started': this.logEvent('$broker.started')(this.scanAllServices),
        '$node.connected': this.logEvent('$node.connected')(this.handleNodeConnection),
        '$node.disconnected': this.logEvent('$node.disconnected')(this.handleNodeDisconnected),
      },
      actions: {
        graphql: {
          params: {
            query: { type: 'string' },
            variables: { type: 'object', optional: true },
          },
          handler: ctx => execute(this.schema, ctx.params.query, null, null, ctx.params.variables),
        },
      },
    });
  }

  removeRemoteSchema(service: ServiceWorker): void {
    const {
      settings: { typeName, schemaCreatedAt },
    } = service;

    const currRemoteSchema = this.remoteSchemas[typeName];
    const outOfDate = currRemoteSchema.schemaCreatedAt < schemaCreatedAt;

    if (outOfDate) {
      delete this.remoteSchemas[typeName];
    }
  }

  buildRemoteSchema(service: ServiceWorker): void {
    const {
      typeName, //
      schemaStr,
      relationships,
      schemaCreatedAt,
      relationDefinitions,
    } = service.settings;

    const remoteSchema = createRemoteSchema({
      service,
      schemaCreatedAt,
      broker: this.broker,
      schema: buildSchema(schemaStr),
    });

    this.remoteSchemas[typeName] = remoteSchema;
  }

  alphabetizeSchema(schema: GraphQLSchema): GraphQLSchema {
    const queryType = schema._queryType;
    const fields = queryType.getFields();
    const unordered = Object.keys(fields);
    const ordered = Object.keys(fields).sort();
    if (JSON.stringify(unordered) !== JSON.stringify(ordered)) {
      const alphabetized = {};
      ordered.forEach(field => {
        alphabetized[field] = fields[field];
      });
      queryType._fields = alphabetized;
      schema._queryType = queryType;
    }
    return schema;
  }

  generateSchema(): GraphQLSchema | void {
    if (isEmpty(this.remoteSchemas)) {
      console.log('[moleculer-graphql] remoteSchemas: empty');
      this.schema = undefined;
      return this.schema;
    }

    const schemas = Object.values(this.remoteSchemas).concat(Object.values(this.relationships));
    const resolvers = buildRelationalResolvers(this.relationDefinitions);

    this.schema = mergeSchemas({ schemas, resolvers });
    this.schema = this.alphabetizeSchema(this.schema);

    if (this.generateSnapshot) {
      this.recordSnapshot();
    }

    return this.schema;
  }

  recordSnapshot(): void {
    if (this.schema) {
      fs.writeFileSync(this.snapshotPath, printSchema(this.schema));
    }
  }
}
