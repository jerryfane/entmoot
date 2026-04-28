import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Entmoot',
  tagline: 'Layer-2 group communication for agents on Pilot',
  favicon: 'img/logo.svg',

  url: 'https://jerryfane.github.io',
  baseUrl: '/entmoot/',
  organizationName: 'jerryfane',
  projectName: 'entmoot',

  onBrokenLinks: 'throw',
  trailingSlash: false,

  future: {
    v4: true,
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  themes: ['@docusaurus/theme-mermaid'],
  plugins: [
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          {
            from: '/',
            to: '/docs/intro',
          },
        ],
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: 'docs',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/jerryfane/entmoot/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/logo.svg',
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'Entmoot',
      logo: {
        alt: 'Entmoot',
        src: 'img/logo.svg',
        href: '/docs/intro',
      },
      items: [
        {type: 'docSidebar', sidebarId: 'docsSidebar', position: 'left', label: 'Docs'},
        {to: '/docs/architecture/system-overview', label: 'Architecture', position: 'left'},
        {to: '/docs/operations/deployment', label: 'Operations', position: 'left'},
        {to: '/docs/reference/configuration', label: 'Reference', position: 'left'},
        {
          href: 'https://github.com/jerryfane/entmoot',
          label: 'GitHub',
          position: 'right',
        },
        {
          href: 'https://github.com/jerryfane/entmoot/releases',
          label: 'Releases',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      logo: {
        alt: 'Entmoot',
        src: 'img/logo.svg',
        href: '/docs/intro',
        width: 40,
        height: 40,
      },
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Introduction', to: '/docs/intro'},
            {label: 'Install', to: '/docs/getting-started/install'},
            {label: 'CLI', to: '/docs/cli/entmootd'},
          ],
        },
        {
          title: 'Operate',
          items: [
            {label: 'Deployment', to: '/docs/operations/deployment'},
            {label: 'Release Checklist', to: '/docs/operations/release-checklist'},
            {label: 'Diagnostics', to: '/docs/operations/diagnostics'},
          ],
        },
        {
          title: 'Reference',
          items: [
            {label: 'Papers', to: '/docs/reference/papers'},
            {label: 'Changelog', href: 'https://github.com/jerryfane/entmoot/blob/main/CHANGELOG.md'},
            {label: 'Pilot', href: 'https://github.com/jerryfane/pilotprotocol'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Entmoot contributors.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
