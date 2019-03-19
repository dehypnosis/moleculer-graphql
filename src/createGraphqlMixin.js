/**
 * @file Generate a mixin with graphql!
 */

import { makeExecutableSchema } from "graphql-tools";
import { graphql as execute } from "graphql";

export const createGraphqlMixin = ({
  schema,
  resolvers,
  relationships,
  relationDefinitions,
  directiveResolvers,
  schemaDirectives,
}) => ({
  settings: {
    schema,
    relationships,
    relationDefinitions,
    hasGraphQLSchema: true
  },
  actions: {
    graphql: {
      params: {
        query: { type: "string" },
        variables: { type: "object", optional: true }
      },
      handler(ctx) {
        return execute(
          this.graphqlSchema,
          ctx.params.query,
          this.resolvers,
          ctx,
          ctx.params.variables
        );
      }
    }
  },
  created() {
    this.resolvers = resolvers;
    this.graphqlSchema = makeExecutableSchema({
      typeDefs: [schema],
      resolvers,
      directiveResolvers,
      schemaDirectives,
    });
  },
  started() {
    setTimeout(
      () =>
        this.broker.broadcast("graphqlService.connected", {
          schema,
          serviceName: this.name,
          relationships,
          relationDefinitions,
        }),
      1000
    );
  },
  stopped() {
    this.broker.broadcast("graphqlService.disconnected", {
      schema,
      serviceName: this.name,
      relationships,
      relationDefinitions,
    });
  }
});
