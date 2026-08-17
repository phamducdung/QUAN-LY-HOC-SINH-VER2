import { useState, useEffect, useCallback } from 'react';
import {
  subscribeSyncStatus,
  initRealtimeSync,
  forceFullSync,
  getSyncStatus,
  SyncStatus
} from '../services/syncService';

export const useCloudSync = () => {
  const [status, setStatus] = useState<SyncStatus>(getSyncStatus());
  const [syncMessage, setSyncMessage] = useState<string | null>(null);

  useEffect(() => {
    // Start real-time sync listeners
    const stopSync = initRealtimeSync();

    // Subscribe to status updates
    const unsubscribeStatus = subscribeSyncStatus((newStatus) => {
      setStatus(newStatus);
    });

    return () => {
      unsubscribeStatus();
      stopSync();
    };
  }, []);

  const pushToCloud = useCallback(async () => {
    setSyncMessage('Đang đồng bộ dữ liệu lên Đám mây (Firebase)...');
    await forceFullSync();
    setSyncMessage('Đã đồng bộ thành công lên Firestore!');
    setTimeout(() => setSyncMessage(null), 3000);
  }, []);

  const pullFromCloud = useCallback(async () => {
    setSyncMessage('Đồng bộ thời gian thực từ Đám mây đang kích hoạt.');
    setTimeout(() => setSyncMessage(null), 3000);
  }, []);

  return {
    isSyncing: status.isSyncing,
    isOnline: status.isOnline,
    lastSyncedAt: status.lastSyncedAt,
    syncErrors: status.syncErrors,
    syncStatus: syncMessage,
    pushToCloud,
    pullFromCloud,
  };
};
