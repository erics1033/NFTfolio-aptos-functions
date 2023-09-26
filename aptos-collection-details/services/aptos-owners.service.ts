import * as admin from 'firebase-admin';
import {
  consoleLog,
  createCollectionInstance,
  createFirestoreBatchInstance,
  errorLog,
  executeWriteBatch,
} from '../../../service';
import { COLLECTION_NAMES } from '../../../values';
import { fetchAptosUniqueOwners } from './graphql.service';

/**
 * 1) Gets all active Aptos collections
 * 2) Fetches unique num_owners for each collection based on on-chain call
 * 3) Updates stats.num_owners for each collection
 *
 * @return {any} success:true/false, error
 */
export const updateAptosCollectionNumOwners = async () => {
  try {
    const updateBatch = createFirestoreBatchInstance();

    const queryData: any = [];

    const topCollectionRef: any = createCollectionInstance(
      COLLECTION_NAMES.TOP_COLLECTIONS
    );

    const queryResult: any = await topCollectionRef
      .where('active', '==', true)
      .where('chain', '==', 'aptos')
      .orderBy(admin.firestore.FieldPath.documentId(), 'desc')
      .get();

    await queryResult.forEach(async (doc: any) => {
      const id = doc.id;
      const data = doc.data();

      queryData.push({ ...data, id });
    });

    // Loops through all the APT collections
    for (let i = 0; i < queryData.length; i++) {
      const collectionData = queryData[i];

      if (
        !collectionData?.verified_creator_address ||
        collectionData?.verified_creator_address == ''
      )
        continue;

      const { data, errors } = await fetchAptosUniqueOwners(
        collectionData?.verified_creator_address
      );

      // Log out and skip over collection if there was an error
      if (errors) {
        consoleLog(
          `${collectionData?.name} - fetchAptosUniqueOwners errors: `,
          errors
        );
        continue;
      }

      consoleLog(
        `${collectionData?.name} - fetchAptosUniqueOwners data: `,
        data
      );

      const updatedNumOwners =
        data?.current_collection_ownership_v2_view_aggregate?.aggregate?.count;

      consoleLog(
        `${collectionData?.name} - new num owners: ${updatedNumOwners}`
      );

      // Updates APT collection's num owners
      if (updatedNumOwners && updatedNumOwners != 0) {
        const ref = createCollectionInstance(
          COLLECTION_NAMES.TOP_COLLECTIONS
        ).doc(collectionData?.id);

        collectionData.stats.num_owners = updatedNumOwners;

        updateBatch.set(ref, collectionData, { merge: true });
      }
    }

    await executeWriteBatch(updateBatch, 'Update aptos num owners data batch');
    return { success: true };
  } catch (error) {
    errorLog('updateAptosCollectionNumOwners | Error ', error);
    return { success: false, error };
  }
};
