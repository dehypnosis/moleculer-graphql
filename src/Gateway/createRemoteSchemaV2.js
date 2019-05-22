/**
 * @file creates a remotely executable schema using Moleculer
 * @flow
 */
import { GraphQLSchema } from 'graphql';
import type { ServiceBroker, Service } from 'moleculer';
import { introspectSchema, makeRemoteExecutableSchema } from 'graphql-tools';

import { MoleculerLink } from './MoleculerLink';

type RemoteSchemaOptions = {
  service: Service,
  broker: ServiceBroker,
  schema: GraphQLSchema,
  schemaCreatedAt: Number,
};

export type ExtendGqlSchema = GraphQLSchema & {
  schemaCreatedAt: Number,
};

export function createRemoteSchema({ broker, service, schema, schemaCreatedAt }: RemoteSchemaOptions): ExtendGqlSchema {
  const link = new MoleculerLink({ broker, service: service.name });
  const remoteSchema = makeRemoteExecutableSchema({ schema, link });
  remoteSchema.schemaCreatedAt = schemaCreatedAt;
  return remoteSchema;
}
