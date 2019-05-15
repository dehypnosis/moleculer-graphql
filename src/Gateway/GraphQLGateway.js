/**
 * @file createGraphQLGateway
 * @author Brad Decker <brad.decker@conciergeauctions.com>
 *
 * The primary purpose of this file is to build a schema that stitches
 * together the schemas of all remote services that have been defined
 * using the graphql mixin also provided by this library.
 * @flow
 */
import { mergeSchemas } from 'graphql-tools';
import { printSchema, parse, graphql as execute } from 'graphql';
import difference from 'lodash.difference';
import selectn from 'selectn';
import fs from 'fs';
import { createRemoteSchema } from './createRemoteSchema';
import { buildRelationalResolvers } from './buildRelationalResolvers';
import { getRelatedTypes } from './utilities';

import type { GraphQLSchema, DocumentNode } from 'graphql';
import type { ServiceBroker, ServiceWorker } from 'moleculer';
import type { TypeRelationDefinitions } from '../Types/ServiceConfiguration';

opaque type ServiceName = string;

type GatewayOptions = {
  broker: ServiceBroker,
  expectedTypes?: Array<string>,
  waitTimeout?: number,
  waitInterval?: number,
  blacklist?: Array<string>,
  generateSnapshot?: boolean,
  snapshotPath?: string,
};

type RemoteSchemaMap = {
  [TypeName: string]: GraphQLSchema,
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
  expectedTypes: Array<string> = [];
  // If true, save a snapshot schema file everytime the schema changes
  generateSnapshot: boolean = false;
  // Boolean to track whether the schema has been initialized
  initialized: boolean = false;
  // Method to hook into service discovery.
  onServiceDiscovery: (service: ServiceWorker) => void;
  // All relationship resolver definitions in the remote schemas
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
  // Length of time in milliseconds to wait for expectedTypes
  waitTimeout: number = 5000;
  // Interval in milliseconds to poll for expectedTypes
  waitInterval: number = 100;
  // Keep track on which service registered

  getGqlServices = (services: Array<any>): Array<any> =>
    services.filter(service => {
      const notInBlackList = !this.blacklist.includes(service.name);
      const hasSchema = service.settings.hasGraphQLSchema;

      return hasSchema && notInBlackList;
    });

  addGql = async (services: Array<any>): Promise<void> => {
    this.getGqlServices(services).map(service => this.buildRemoteSchema(service));
    this.generateSchema();
  };

  removeGql = (services: any): void => {
    services.map(service => this.removeRemoteSchema(service));
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

  handleNodeConnection = ({ node }: Object): Promise<void> => {
    if (!node) {
      console.log('[gateway][handleNodeConnection] node: empty');
      return Promise.resolve();
    }

    return this.addGql(node.services);
  };

  // When nodes disconnect we scan their services for schemas and remove them
  handleNodeDisconnected = async ({ node }: Object): Promise<void> => {
    if (!node) {
      console.log('[gateway][handleNodeDisconnected] node: empty');
      return Promise.resolve();
    }

    this.removeGql(node.services);
  };

  constructor(opts: GatewayOptions) {
    this.broker = opts.broker;
    if (opts.expectedTypes) this.expectedTypes = opts.expectedTypes;
    if (opts.waitInterval) this.waitInterval = opts.waitInterval;
    if (opts.waitTimeout) this.waitTimeout = opts.waitTimeout;
    if (opts.blacklist) this.blacklist.concat(opts.blacklist);
    if (opts.generateSnapshot) this.generateSnapshot = opts.generateSnapshot;
    if (opts.snapshotPath) this.snapshotPath = opts.snapshotPath;
    if (opts.onServiceDiscovery) this.onServiceDiscovery = opts.onServiceDiscovery;

    this.service = this.broker.createService({
      name: '@gateway',
      events: {
        '$gql.added': this.handleGqlAdded,
        '$gql.removed': this.handleGqlRemoved,

        // * Event "$services.changed" thrown when broker load service
        // * We can't trust in this hook to merge schema
        // '$services.changed': this.handleServiceUpdate,
        '$node.connected': this.handleNodeConnection,
        '$node.disconnected': this.handleNodeDisconnected,
        '$node.disconnected': this.handleNodeDisconnected,
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

  removeRemoteSchema(service: ServiceWorker): void {
    const {
      settings: { typeName, relationships, relationDefinitions },
    } = service;

    delete this.remoteSchemas[typeName];

    if (relationships) {
      delete this.relationships[typeName];
      delete this.relationDefinitions[typeName];

      // TODO: Revert expected types. Now we don't use this logic in app
      const relatedTypes = getRelatedTypes(parse(relationships));
      const missingTypes = difference(relatedTypes, this.discoveredTypes);
      this.expectedTypes = this.expectedTypes.concat(missingTypes);
    }
  }

  buildRemoteSchema(service: ServiceWorker): void {
    const {
      settings: { typeName, schema, relationships, relationDefinitions },
    } = service;

    this.remoteSchemas[typeName] = schema;

    if (relationships) {
      this.relationships[typeName] = relationships;
      this.relationDefinitions[typeName] = relationDefinitions;
      const relatedTypes = getRelatedTypes(parse(relationships));
      const missingTypes = difference(relatedTypes, this.discoveredTypes);
      this.expectedTypes = this.expectedTypes.concat(missingTypes);
    }
  }

  generateSchema(): GraphQLSchema {
    const schemas = Object.values(this.remoteSchemas).concat(Object.values(this.relationships));
    const resolvers = buildRelationalResolvers(this.relationDefinitions);
    this.schema = mergeSchemas({
      schemas,
      resolvers,
    });
    this.schema = this.alphabetizeSchema(this.schema);
    if (this.generateSnapshot) this.recordSnapshot();
    return this.schema;
  }

  recordSnapshot(): void {
    if (this.schema) {
      fs.writeFileSync(this.snapshotPath, printSchema(this.schema));
    }
  }

  /**
   * Wait for services expected
   */
  start(): Promise<GraphQLSchema> {
    return new Promise((resolve, reject) => {
      const maxTries = this.waitTimeout / this.waitInterval;
      let tries = 0;
      this.timer = setInterval(() => {
        tries++;
        if (tries >= maxTries) {
          reject(new Error('Timeout'));
        }
        const discoveredTypes = Object.keys(this.discoveredTypes);
        const undiscovered = difference(this.expectedTypes, discoveredTypes);
        if (discoveredTypes.length === 0) return;
        if (discoveredTypes.some(type => !this.remoteSchemas[type])) return;
        if (undiscovered.length > 0) {
          if (this.broker.logger) {
            const msg = `Still waiting for ${undiscovered.join(', ')} types to be discovered`;
            this.broker.logger.warn(msg);
          }
          return;
        }
        clearInterval(this.timer);
        this.generateSchema();
        resolve(this.schema);
      }, this.waitInterval);
    });
  }
}
