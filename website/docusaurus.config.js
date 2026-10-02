// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const fs = require('node:fs/promises');
const path = require('node:path');
const {themes: prismThemes} = require('prism-react-renderer');

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

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // add new pages here
    plugins: [
      // Copies the hand-written Webflow pages in `website/homepage/`
      // (`index.html` and `404.html`) into the site root, after Docusaurus has
      // written its own output, so they take precedence over the generated
      // pages of the same name. Their CSS, JS, fonts and images stay in
      // `static/` and are merged into the same output directory by the build;
      // a re-export from Webflow has to be split the same way.
      //
      // The pages cannot live in `static/`: the dev server serves static
      // directories alongside webpack's own `index.html`, and the duplicate
      // asset name fails compilation with "Conflict: Multiple assets emit
      // different content to the same filename index.html", which stops hot
      // reload ("Reload prevented"). Copying after the build is also what keeps
      // the hand-written `404.html`: placed in `static/` it survived on
      // Docusaurus 3.0, but on 3.10 the generated 404 page overwrites it.
      function webflowRootPages() {
        return {
          name: 'xtable-webflow-root-pages',
          async postBuild({outDir}) {
            const srcDir = path.join(__dirname, 'homepage');
            const files = await fs.readdir(srcDir);
            // Overwriting the generated 404.html is the point; anything else
            // means a Docusaurus page is being shadowed, so say so out loud —
            // moving these files out of `static/` gave up webpack's own
            // duplicate-asset error.
            for (const file of files) {
              const generated = await fs
                .access(path.join(outDir, file))
                .then(() => true, () => false);
              if (generated && file !== '404.html') {
                console.warn(
                  `[WARNING] homepage/${file} overwrites a generated page of the same name.`,
                );
              }
            }
            await fs.cp(srcDir, outDir, {recursive: true});
          },
        };
      },
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
                // Plain href (not `pathname:///`): the prefix is only resolved
                // in `to`/`href` config fields, not inside raw html, and a full
                // page load is what serves the hand-written home page at `/`.
                html: '<a href="/" target="_self" class="footer__logo-link"><img src="/images/xtable-white.png" alt="Apache XTable™ (Incubating)" class="footer__xtable-logo" width="170" /></a>',
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
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
      },
    }),
};

module.exports = config;
