import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'category',
      label: 'Getting started',
      collapsed: false,
      items: ['introduction', 'installation', 'configuration', 'your-first-workflow'],
    },
    {
      type: 'category',
      label: 'Core concepts',
      collapsed: false,
      items: [
        'actions',
        'sagas-and-compensation',
        'signals',
        'side-effects',
        'parallel',
        'optional-actions',
        'child-workflows',
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      items: [
        'tags-and-querying',
        'expiration-and-monitoring',
        'queues-locks-idempotency',
        'synchronous-execution',
        'artisan-commands',
      ],
    },
    {
      type: 'category',
      label: 'Advanced',
      items: [
        'versioning',
        'octane-and-multi-tenancy',
        'determinism-rules',
        'events',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: ['testing'],
    },
  ],
};

export default sidebars;
