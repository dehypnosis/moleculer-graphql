/**
 * @file creates a remotely executable schema using Moleculer
 * @flow
 */
import { GraphQLSchema } from 'graphql';
import type { ServiceBroker, Service } from 'moleculer';
import { introspectSchema, makeRemoteExecutableSchema } from 'graphql-tools';

import { MoleculerLink } from './MoleculerLink';

type RemoteSchemaOptions = {
  broker: ServiceBroker,
  service: Service,
  schema: GraphQLSchema,
};

export function createRemoteSchema({ broker, service, schema }: RemoteSchemaOptions) {
  const link = new MoleculerLink({ broker, service: service.name });
  return makeRemoteExecutableSchema({ schema, link });
}
