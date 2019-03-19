/**
 * @file buildRelationalResolvers
 * @author Brad Decker <brad.decker@conciergeauctions.com>
 * @flow
 *
 * This file defines a set of functions used to generate relationship
 * resolvers for the stitched together relational schema generated by
 * the GraphQLGateway and moleculer network.
 */
import selectn from "selectn";
import type {
  RelationDefinition,
  RelationDefinitions,
  ArgumentDefinitionMap,
  TypeRelationDefinitions
} from "../Types/ServiceConfiguration";

type ResolverProps = {
  parent: Object,
  args: Object,
  context: Object,
  info: Object
};

type ResolverArgs = {
  [key: string]: any
};

/**
 * @function getArgs
 * Builds an object of arguments to send along with the query.
 * The arguments are built by following the dot notation path
 * provided by the service in the relationships configurations.
 */
const getArgs = (
  relationship: RelationDefinition,
  props: ResolverProps
): ResolverArgs => {
  if (!relationship.args) return {};
  const computedArgs = {};
  for (const arg of Object.keys(relationship.args)) {
    computedArgs[arg] = selectn(relationship.args[arg], props);
  }
  return computedArgs;
};

/**
 * @function getFieldResolvers
 * Gets the resolvers for the fields on a given type. This function
 * loops through all the fields defined for a type and returns an
 * object with a resolve method that calls mergeInfo.delegate to
 * stitch together remote schemas.
 */
const getFieldResolvers = (
  relationships: RelationDefinitions,
  mergeInfo: MergeInfo
): IResolvers => {
  const relationshipResolvers = {};
  for (const fieldName of Object.keys(relationships)) {
    const definition = relationships[fieldName];

    relationshipResolvers[fieldName] = {
      fragment: definition.fragment,
      resolve(parent, args, context, info) {
        return mergeInfo.delegate(
          definition.type,
          definition.operationName,
          getArgs(definition, { parent, args, context, info }),
          context,
          info
        );
      }
    };
  }
  return relationshipResolvers;
};

/**
 * @function generateResolvers
 * Builds a type map of resolvers that utilize mergeInfo to
 * stitch together the remote schemas they are attached to.
 */
export function buildRelationalResolvers(
  typeDefinitions: TypeRelationDefinitions,
  validatedRelationalTypes?: Array<string>
): getMergeSchemaResolver {
  return mergeInfo => {
    const typeResolvers = {};
    for (const type of Object.keys(typeDefinitions)) {
      if (validatedRelationalTypes && !validatedRelationalTypes.includes(type))
        continue;
      typeResolvers[type] = getFieldResolvers(typeDefinitions[type], mergeInfo);
    }

    return typeResolvers;
  };
}
