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
            items: [
              {
                html: '<a href="pathname:///" class="footer__logo-link"><img src="/images/xtable-white.png" alt="Apache XTable™ (Incubating)" class="footer__xtable-logo" width="170" /></a>',
              },
            ],
          },
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
        copyright:
          '<a href="https://incubator.apache.org/" target="_blank" rel="noopener"><img src="https://www.apache.org/logos/res/incubator/incubator.png" alt="Apache Incubator" width="150" style="margin:12px 0;background:#ffffff;padding:4px;border-radius:4px;" /></a><br/>' +
          'Apache XTable™ is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF. ' +
          'Copyright © ' + new Date().getFullYear() + ' Apache XTable™, XTable, Apache, the Apache feather logo and the Apache XTable™ project logo are either registered trademarks or trademarks of The Apache Software Foundation in the United States and other countries.',
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
