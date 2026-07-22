// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Apache XTable™ (Incubating)',
  favicon: 'images/xtable-favicon.png',
  url: 'https://xtable.apache.org',
  baseUrl: '/',

  // GitHub pages deployment config.
  organizationName: 'apache',
  projectName: 'incubator-xtable',

  onBrokenLinks: 'ignore',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // add new pages here
    plugins: [
      [
        '@docusaurus/plugin-content-docs',
        {
          id: 'releases',
          path: 'releases',
          routeBasePath: 'releases',
        },
      ],
      [
        '@docusaurus/plugin-content-docs',
        {
          id: 'community',
          path: 'community',
          routeBasePath: 'community',
        },
      ]
    ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
        },
        blog: {
          showReadingTime: true,
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Apache XTable™ (Incubating)',
        logo: {
          alt: 'Apache XTable™ (Incubating) Logo',
          href: 'pathname:///',
          target: '_self',
          src: 'images/xtable-icon.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docs',
            position: 'left',
            label: 'Docs',
          },
          {
            href: 'https://github.com/apache/incubator-xtable',
            label: 'GitHub',
            position: 'right',
          },
          {to: 'blog', label: 'Blogs', position: 'left'},
          {to: 'releases/downloads', label: 'Downloads', position: 'left'},
          {to: 'community/sync', label: 'Community', position: 'left'}
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'XTable',
            items: [
              {label: 'Docs', to: '/docs/setup/'},
              {label: 'Blogs', to: '/blog'},
              {label: 'Downloads', to: '/releases/downloads'},
              {label: 'Community', to: '/community/sync'},
              {label: 'GitHub', href: 'https://github.com/apache/incubator-xtable'},
            ],
          },
          {
            title: 'Apache Software Foundation',
            items: [
              {label: 'Foundation', href: 'https://www.apache.org/'},
              {label: 'License', href: 'https://www.apache.org/licenses/'},
              {label: 'Events', href: 'https://www.apache.org/events/current-event'},
              {label: 'Sponsorship', href: 'https://www.apache.org/foundation/sponsorship.html'},
              {label: 'Thanks', href: 'https://www.apache.org/foundation/thanks.html'},
              {label: 'Security', href: 'https://www.apache.org/security/'},
              {label: 'Privacy', href: 'https://privacy.apache.org/policies/privacy-policy-public.html'},
            ],
          },
        ],
        logo: {
          alt: 'Apache Incubator',
          src: 'https://www.apache.org/logos/res/incubator/incubator.png',
          href: 'https://incubator.apache.org/',
          width: 150,
        },
        copyright:
          'Apache XTable™ is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. ' +
          'Copyright © ' + new Date().getFullYear() + ' The Apache Software Foundation. ' +
          'Apache XTable™, XTable, Apache, the Apache feather logo and the Apache XTable™ project logo are either registered trademarks or trademarks of The Apache Software Foundation in the United States and other countries. ' +
          'Licensed under the Apache License, Version 2.0.',
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
