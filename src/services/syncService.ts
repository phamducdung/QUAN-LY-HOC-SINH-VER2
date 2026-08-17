import {
  collection,
  doc,
  setDoc,
  deleteDoc,
  getDoc,
  getDocs,
  onSnapshot,
  Unsubscribe,
  query,
  limit
} from 'firebase/firestore';
import { db as firestoreDb, handleFirestoreError } from '../firebase';
import { db as dexieDb, isRemoteSyncing, setRemoteSyncing } from '../db/dexie';

export interface SyncStatus {
  isOnline: boolean;
  isSyncing: boolean;
  lastSyncedAt: Date | null;
  syncErrors: number;
}

const ALL_TABLES = [
  'school_years',
  'classes',
  'students',
  'class_students',
  'sessions',
  'student_sessions',
  'knowledge_tags',
  'knowledge_results',
  'warnings',
  'ai_diagnoses',
  'audit_logs',
  'settings'
] as const;

type TableName = typeof ALL_TABLES[number];

let unsubscribers: Unsubscribe[] = [];
let statusListeners: Array<(status: SyncStatus) => void> = [];

let currentSyncStatus: SyncStatus = {
  isOnline: typeof navigator !== 'undefined' ? navigator.onLine : true,
  isSyncing: false,
  lastSyncedAt: null,
  syncErrors: 0,
};

function updateSyncStatus(patch: Partial<SyncStatus>) {
  currentSyncStatus = { ...currentSyncStatus, ...patch };
  statusListeners.forEach((fn) => fn(currentSyncStatus));
}

export function subscribeSyncStatus(listener: (status: SyncStatus) => void) {
  statusListeners.push(listener);
  listener(currentSyncStatus);
  return () => {
    statusListeners = statusListeners.filter((fn) => fn !== listener);
  };
}

export function getSyncStatus() {
  return currentSyncStatus;
}

// Push a single record change to Firestore
export async function pushDocToFirestore(
  tableName: TableName,
  docId: string,
  data?: any,
  isDelete = false
) {
  if (isRemoteSyncing) return;
  if (!firestoreDb) return;

  try {
    const docRef = doc(firestoreDb, tableName, docId);
    if (isDelete) {
      await deleteDoc(docRef);
    } else if (data) {
      // Ensure doc has an updated_at timestamp if missing
      const cleanData = {
        ...data,
        id: docId,
        updated_at: data.updated_at || data.last_updated || new Date().toISOString()
      };
      // Clean undefined values for Firestore compatibility
      Object.keys(cleanData).forEach((key) => {
        if (cleanData[key] === undefined) {
          delete cleanData[key];
        }
      });
      await setDoc(docRef, cleanData, { merge: true });
    }
    updateSyncStatus({ lastSyncedAt: new Date() });
  } catch (err: any) {
    handleFirestoreError(err, `Push ${tableName}/${docId}`);
    updateSyncStatus({ syncErrors: currentSyncStatus.syncErrors + 1 });
  }
}

// Real-time listener setup for all Firestore collections -> Dexie
export function initRealtimeSync(): () => void {
  stopRealtimeSync();

  if (!firestoreDb) {
    console.warn('[SyncService] Firestore DB not initialized');
    return () => {};
  }

  // Monitor browser online/offline status
  const handleOnline = () => updateSyncStatus({ isOnline: true });
  const handleOffline = () => updateSyncStatus({ isOnline: false });

  if (typeof window !== 'undefined') {
    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);
  }

  updateSyncStatus({ isSyncing: true });

  ALL_TABLES.forEach((tableName) => {
    try {
      const colRef = collection(firestoreDb, tableName);
      const unsub = onSnapshot(
        colRef,
        async (snapshot) => {
          if (snapshot.metadata.hasPendingWrites) {
            // Local writes handled locally, skip echo
            return;
          }

          setRemoteSyncing(true);
          try {
            const dexieTable = (dexieDb as any)[tableName];
            if (!dexieTable) return;

            for (const change of snapshot.docChanges()) {
              const docId = change.doc.id;
              const docData = change.doc.data();

              if (change.type === 'removed') {
                await dexieTable.delete(docId);
              } else if (change.type === 'added' || change.type === 'modified') {
                const existing = await dexieTable.get(docId);

                // Conflict Resolution: Last-Write-Wins based on updated_at
                if (existing) {
                  const localTime = new Date(existing.updated_at || existing.last_updated || 0).getTime();
                  const remoteTime = new Date(docData.updated_at || docData.last_updated || 0).getTime();

                  if (remoteTime >= localTime) {
                    await dexieTable.put({ ...docData, id: docId });
                  }
                } else {
                  await dexieTable.put({ ...docData, id: docId });
                }
              }
            }
            updateSyncStatus({ lastSyncedAt: new Date(), isSyncing: false });
          } catch (err: any) {
            console.error(`[SyncService] Error processing snapshot for ${tableName}:`, err);
            updateSyncStatus({ syncErrors: currentSyncStatus.syncErrors + 1, isSyncing: false });
          } finally {
            setRemoteSyncing(false);
          }
        },
        (error) => {
          handleFirestoreError(error, `Listener ${tableName}`);
          updateSyncStatus({ syncErrors: currentSyncStatus.syncErrors + 1, isSyncing: false });
        }
      );
      unsubscribers.push(unsub);
    } catch (err: any) {
      handleFirestoreError(err, `Init Listener ${tableName}`);
    }
  });

  return stopRealtimeSync;
}

export function stopRealtimeSync() {
  unsubscribers.forEach((unsub) => {
    try {
      unsub();
    } catch (e) {
      // ignore
    }
  });
  unsubscribers = [];

  if (typeof window !== 'undefined') {
    window.removeEventListener('online', () => updateSyncStatus({ isOnline: true }));
    window.removeEventListener('offline', () => updateSyncStatus({ isOnline: false }));
  }
}

// Force full sync: push local Dexie records to Firestore
export async function forceFullSync() {
  if (!firestoreDb) return;
  updateSyncStatus({ isSyncing: true });

  try {
    for (const tableName of ALL_TABLES) {
      const dexieTable = (dexieDb as any)[tableName];
      if (!dexieTable) continue;

      const items = await dexieTable.toArray();
      for (const item of items) {
        if (!item.id) continue;
        await pushDocToFirestore(tableName, String(item.id), item, false);
      }
    }
    updateSyncStatus({ lastSyncedAt: new Date(), isSyncing: false });
  } catch (err: any) {
    handleFirestoreError(err, 'forceFullSync');
    updateSyncStatus({ syncErrors: currentSyncStatus.syncErrors + 1, isSyncing: false });
  }
}

// Clear all Firestore collections for real-world blank slate deployment
export async function clearFirestoreDatabase() {
  if (!firestoreDb) return;
  try {
    for (const tableName of ALL_TABLES) {
      if (tableName === 'knowledge_tags' || tableName === 'settings') continue;
      const colRef = collection(firestoreDb, tableName);
      const snap = await getDocs(colRef);
      for (const docSnapshot of snap.docs) {
        await deleteDoc(docSnapshot.ref);
      }
    }
  } catch (err: any) {
    handleFirestoreError(err, 'clearFirestoreDatabase');
  }
}

