import { resourceTypeTheme, tabStyle, tabStripStyle } from '../app/ui';
import type { ResourceType } from '../app/ui';

interface ResourceTabsProps {
  activeTab: ResourceType;
  counts: Record<ResourceType, number>;
  onChange: (tab: ResourceType) => void;
}

const TABS: ResourceType[] = ['log', 'metric', 'xray'];

export default function ResourceTabs({ activeTab, counts, onChange }: ResourceTabsProps) {
  return (
    <div style={tabStripStyle}>
      {TABS.map((type) => {
        const theme = resourceTypeTheme(type);
        const active = activeTab === type;
        const count = counts[type];
        const badgeBg = active ? theme.badgeBg : '#f1f5f9';
        const badgeColor = active ? theme.badgeColor : '#64748b';
        return (
          <button key={type} type="button" style={tabStyle(active)} onClick={() => onChange(type)}>
            <span>{theme.icon}</span>
            <span>{theme.label}</span>
            <span style={{
              background: badgeBg,
              color: badgeColor,
              borderRadius: 10,
              fontSize: 10,
              padding: '1px 6px',
              fontWeight: 600,
            }}>
              {count}
            </span>
          </button>
        );
      })}
    </div>
  );
}
