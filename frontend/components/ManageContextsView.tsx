import { useState } from 'react';
import IncidentContextDetails from './IncidentContextDetails';
import IncidentContextForm from './IncidentContextForm';
import IncidentContextList from './IncidentContextList';
import Modal from './Modal';
import type { IncidentContext, IncidentContextUpsertPayload } from '../types/incidentContext';
import { buttonStyle } from '../app/ui';

interface Props {
  apiUrl: string;
  contexts: IncidentContext[];
  contextsLoading: boolean;
  contextsError: string | null;
  selectedContext: IncidentContext | null;
  contextSubmitting: boolean;
  onSelectContext: (contextId: string) => Promise<void>;
  onCreateContext: (payload: IncidentContextUpsertPayload) => Promise<void>;
  onUpdateContext: (payload: IncidentContextUpsertPayload) => Promise<void>;
  onDeleteContext: () => Promise<void>;
}

type ModalMode = null | 'create' | 'edit';

export default function ManageContextsView({
  apiUrl,
  contexts,
  contextsLoading,
  contextsError,
  selectedContext,
  contextSubmitting,
  onSelectContext,
  onCreateContext,
  onUpdateContext,
  onDeleteContext,
}: Props) {
  const [modalMode, setModalMode] = useState<ModalMode>(null);

  return (
    <section>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
        <h2 style={{ margin: 0, fontSize: '1.15rem', color: '#1a2438' }}>Incident Contexts</h2>
        <button
          type="button"
          onClick={() => setModalMode('create')}
          style={buttonStyle('primary')}
        >
          + New Context
        </button>
      </div>
      <p style={{ color: '#667085', fontSize: '0.92rem', marginBottom: 16 }}>
        Define which resources to investigate — log groups, metrics, and X-Ray services.
      </p>

      {contextsError && <p style={{ color: '#c00', fontSize: '0.9rem' }}>{contextsError}</p>}

      {contextsLoading ? (
        <p style={{ color: '#667085' }}>Loading contexts...</p>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: '280px 1fr', gap: 16 }}>
          <IncidentContextList
            items={contexts}
            selectedContextId={selectedContext?.contextId ?? null}
            onSelect={onSelectContext}
          />

          <div>
            {selectedContext && (
              <IncidentContextDetails
                context={selectedContext}
                onEdit={() => setModalMode('edit')}
                onDelete={() => void onDeleteContext()}
                deleteDisabled={contextSubmitting}
              />
            )}

            {!selectedContext && (
              <div style={{
                border: '1px dashed #d0d5dd',
                borderRadius: 12,
                padding: '32px 20px',
                textAlign: 'center',
                color: '#98a2b3',
              }}>
                Select a context to view details, or create a new one.
              </div>
            )}
          </div>
        </div>
      )}

      <Modal
        open={modalMode === 'create'}
        onClose={() => setModalMode(null)}
        title="Create Context"
        width={680}
      >
        <IncidentContextForm
          investigationsApiUrl={apiUrl}
          mode="create"
          submitting={contextSubmitting}
          onSubmit={async (payload) => {
            await onCreateContext(payload);
            setModalMode(null);
          }}
          onCancelEdit={() => setModalMode(null)}
        />
      </Modal>

      <Modal
        open={modalMode === 'edit'}
        onClose={() => setModalMode(null)}
        title="Edit Context"
        width={680}
      >
        {selectedContext && (
          <IncidentContextForm
            investigationsApiUrl={apiUrl}
            mode="edit"
            initialValue={selectedContext}
            submitting={contextSubmitting}
            onSubmit={async (payload) => {
              await onUpdateContext(payload);
              setModalMode(null);
            }}
            onCancelEdit={() => setModalMode(null)}
          />
        )}
      </Modal>
    </section>
  );
}
