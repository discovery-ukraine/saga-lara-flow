import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import { themes as prismThemes } from 'prism-react-renderer';

const config: Config = {
  title: 'Saga Lara Flow',
  tagline: 'A workflow engine with an integrated Saga pattern for Laravel Queues',
  favicon: 'img/favicon.ico',

  headTags: [
    {
      tagName: 'link',
      attributes: {
        rel: 'apple-touch-icon',
        sizes: '180x180',
        href: '/img/apple-touch-icon.png',
      },
    },
  ],

  // Production URL and base path. Served at the root of the custom domain.
  url: 'https://sagalaraflow.dev',
  baseUrl: '/',
  trailingSlash: false,

  organizationName: 'discovery-ukraine',
  projectName: 'saga-lara-flow',

  onBrokenLinks: 'throw',

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          // Docs are served at the site root (no /docs prefix).
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/discovery-ukraine/saga-lara-flow/tree/main/docs/',
          // To cut a fixed version later, run `npm run docusaurus docs:version 1.0`
          // and Docusaurus will add the version dropdown automatically.
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: [
    [
      // Offline/local search that works on a static GitHub Pages host — no server.
      // Swap to Algolia DocSearch (see the commented themeConfig.algolia block below)
      // once the DocSearch application is approved for sagalaraflow.dev.
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        indexBlog: false,
        docsRouteBasePath: '/',
        highlightSearchTermsOnTargetPage: true,
      },
    ],
  ],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Saga Lara Flow',
      logo: {
        alt: 'Saga Lara Flow logo',
        src: 'img/logo.png',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/discovery-ukraine/saga-lara-flow',
          label: 'GitHub',
          position: 'right',
        },
        {
          href: 'https://packagist.org/packages/discovery-ukraine/saga-lara-flow',
          label: 'Packagist',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            { label: 'Introduction', to: '/' },
            { label: 'Installation', to: '/installation' },
            { label: 'Your first workflow', to: '/your-first-workflow' },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/discovery-ukraine/saga-lara-flow',
            },
            {
              label: 'Packagist',
              href: 'https://packagist.org/packages/discovery-ukraine/saga-lara-flow',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} discovery-ukraine. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['php', 'bash', 'json'],
    },
    // Enable after Algolia DocSearch approval, then remove the local-search theme above.
    // algolia: {
    //   appId: 'YOUR_APP_ID',
    //   apiKey: 'YOUR_SEARCH_API_KEY',
    //   indexName: 'sagalaraflow',
    //   contextualSearch: true,
    // },
  } satisfies Preset.ThemeConfig,
};

export default config;
