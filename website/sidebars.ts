import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/install',
        'getting-started/create-or-join-group',
        'getting-started/publish-and-query',
        'getting-started/three-peer-mesh',
      ],
    },
    {
      type: 'category',
      label: 'Core Concepts',
      items: [
        'concepts/what-entmoot-is',
        'concepts/libp2p-integration',
        'concepts/groups-rosters-invites',
        'concepts/public-moot-directory',
        'concepts/messages-topics-merkle',
        'concepts/gossip-reconciliation',
        'concepts/connectivity-profiles',
        'concepts/esp',
      ],
    },
    {
      type: 'category',
      label: 'CLI',
      items: [
        'cli/entmootd',
        'cli/join',
        'cli/serve',
        'cli/relay',
        'cli/publish',
        'cli/tail-query-info-version',
        'cli/bootstrap-agent',
        'cli/default-moot',
        'cli/agent-live',
        'cli/mailbox',
        'cli/esp-serve',
        'cli/esp-device-sign-request',
        'cli/founder-commands',
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      items: [
        'operations/deployment',
        'operations/release-checklist',
        'operations/peer-upgrades',
        'operations/diagnostics',
        'operations/search-ui-qa',
        'operations/file-layout-backups',
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      items: [
        'architecture/system-overview',
        'architecture/security-model',
        'architecture/reconciliation',
        'architecture/discovery-relay-tradeoffs',
        'architecture/esp-mobile',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/configuration',
        'reference/json-formats',
        'reference/exit-codes',
        'reference/http-esp-api',
        'reference/changelog',
        'reference/papers',
      ],
    },
  ],
};

export default sidebars;
