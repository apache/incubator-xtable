/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

module.exports = {
    docs: [
        "setup",
        {
            type: 'category',
            label: 'Quick Start',
            collapsed: true,
            items: [
                'how-to'
            ],
        },
        {
            type: 'category',
            label: 'Integrations',
            collapsed: true,
            items: [
                {
                    type: 'category',
                    label: 'Catalogs',
                    link: {
                        type: 'doc',
                        id: 'catalogs-index'
                    },
                    items: [
                        'hms',
                        'glue-catalog',
                        'unity-catalog',
                        'biglake-metastore',
                    ],
                },
                {
                    type: 'category',
                    label: 'Query Engines',
                    link: {
                        type: 'doc',
                        id: 'query-engines-index'
                    },
                    items: [
                        'athena',
                        'redshift',
                        'spark',
                        'presto',
                        'snowflake',
                        'trino',
                    ],
                }
            ]
        },
    ],
};