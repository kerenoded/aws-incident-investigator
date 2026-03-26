import { useEffect, useRef, type CSSProperties, type ReactNode } from 'react';

interface ModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  children: ReactNode;
  width?: number;
}

export default function Modal({ open, onClose, title, children, width = 640 }: ModalProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (open && !dialog.open) dialog.showModal();
    if (!open && dialog.open) dialog.close();
  }, [open]);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    const handleClose = () => onClose();
    dialog.addEventListener('close', handleClose);
    return () => dialog.removeEventListener('close', handleClose);
  }, [onClose]);

  if (!open) return null;

  return (
    <dialog ref={dialogRef} style={overlayStyle} onClick={(e) => { if (e.target === dialogRef.current) onClose(); }}>
      <div style={{ ...panelStyle, maxWidth: width }}>
        <div style={headerStyle}>
          <h2 style={{ margin: 0, fontSize: '1.15rem', color: '#1a2438' }}>{title}</h2>
          <button type="button" onClick={onClose} style={closeButtonStyle} aria-label="Close">&times;</button>
        </div>
        <div style={bodyStyle}>
          {children}
        </div>
      </div>
    </dialog>
  );
}

const overlayStyle: CSSProperties = {
  position: 'fixed',
  inset: 0,
  border: 'none',
  padding: 0,
  maxWidth: '100vw',
  maxHeight: '100vh',
  width: '100vw',
  height: '100vh',
  background: 'rgba(15, 23, 42, 0.45)',
  backdropFilter: 'blur(2px)',
  display: 'flex',
  alignItems: 'flex-start',
  justifyContent: 'center',
  paddingTop: '8vh',
};

const panelStyle: CSSProperties = {
  background: '#fff',
  borderRadius: 14,
  boxShadow: '0 8px 32px rgba(15, 23, 42, 0.18)',
  width: '92%',
  maxHeight: '80vh',
  display: 'flex',
  flexDirection: 'column',
  overflow: 'hidden',
};

const headerStyle: CSSProperties = {
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  padding: '16px 20px',
  borderBottom: '1px solid #eef0f5',
};

const closeButtonStyle: CSSProperties = {
  background: 'none',
  border: 'none',
  fontSize: '1.4rem',
  lineHeight: 1,
  cursor: 'pointer',
  color: '#667085',
  padding: '2px 6px',
  borderRadius: 6,
};

const bodyStyle: CSSProperties = {
  padding: '16px 20px 20px',
  overflowY: 'auto',
};
