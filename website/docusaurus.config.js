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
          {
            label: 'ASF',
            position: 'left',
            items: [
              {
                label: "Foundation",
                to: "https://www.apache.org/",
              },
              {
                label: "License",
                to: "https://www.apache.org/licenses/",
              },
              {
                label: "Events",
                to: "https://www.apache.org/events/current-event",
              },
              {
                label: "Security",
                to: "https://www.apache.org/security/",
              },
              {
                label: "Sponsorship",
                to: "https://www.apache.org/foundation/sponsorship.html",
              },
              {
                label: 'Privacy',
                to: 'https://www.apache.org/foundation/policies/privacy.html'
              },
              {
                label: "Thanks",
                to: "https://www.apache.org/foundation/thanks.html",
              },
            ],
          },
        ],
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
