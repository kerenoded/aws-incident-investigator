import { Component, type ErrorInfo, type ReactNode } from 'react';

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  message: string;
}

export default class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, message: '' };
  }

  static getDerivedStateFromError(error: unknown): State {
    const message = error instanceof Error ? error.message : String(error);
    return { hasError: true, message };
  }

  componentDidCatch(error: unknown, info: ErrorInfo): void {
    console.error('[ErrorBoundary] Unhandled render error:', error, info.componentStack);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{ padding: 16, border: '1px solid #c00', borderRadius: 4, color: '#c00' }}>
          <strong>Something went wrong displaying the report.</strong>
          {this.state.message && (
            <p style={{ marginTop: 8, fontSize: '0.875rem' }}>{this.state.message}</p>
          )}
        </div>
      );
    }
    return this.props.children;
  }
}
