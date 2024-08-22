<!--
 - Licensed to the Apache Software Foundation (ASF) under one
 - or more contributor license agreements.  See the NOTICE file
 - distributed with this work for additional information
 - regarding copyright ownership.  The ASF licenses this file
 - to you under the Apache License, Version 2.0 (the
 - "License"); you may not use this file except in compliance
 - with the License.  You may obtain a copy of the License at
 - 
 -     http://www.apache.org/licenses/LICENSE-2.0
 - 
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and 
 - limitations under the License.
-->

# Apache XTable™ (Incubating) Website Source Code

This repo hosts the source code of [Apache XTable™ (Incubating)](https://github.com/apache/incubator-xtable)

## Prerequisite

Install [npm](https://treehouse.github.io/installation-guides/mac/node-mac.html) for the first time.

## Installation

```console
cd website
npm install
```

## Local Development

```console
cd website
npm start
```

This command starts a local development server and opens up a browser window. 
Most changes are reflected live without having to restart the server.

## Build

```console
cd website
npm run build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Testing your Build Locally
It is important to test your build locally before deploying to production.

```console
cd website
npm run serve
```
> [!NOTE]  
> If you make changes to the contents inside `docs` directory, you can verify the changes locally by visiting https://localhost:3000/docs/setup after running `npm run serve`. 

## Creating new pages with navbar access
1. Create a new folder i.e. `releases` inside the `website` directory
2. Create a new file i.e. `downloads.mdx` inside the `releases` directory
3. Add necessary content to the markdown file
4. Include the reference to the new folder in the `docusaurus.config.js` file as follows:
* Add the new folder to the `plugins` array under `config` key, like
```javascript
      [
        '@docusaurus/plugin-content-docs',
        {
          id: 'releases',
          path: 'releases',
          routeBasePath: 'releases',
        },
      ]
```
* Add the new folder to the `items` array under `navbar` key, like
```javascript
      {to: 'releases/downloads', label: 'Downloads', position: 'left'}
```

## Docs
### Creating new docs
1. Place the new file into the `docs` folder.
2. Include the reference for the new file into the `sidebars.js` file.

## Blogs
### Adding new external blogs (redirection)
1. Create a `.mdx` file similar to `website/blog/onetable-now-oss.mdx`
2. Add relevant thumbnail image to `website/static/images/<folder-name>` folder
3. Make sure to add the appropriate redirection link in the `.mdx` file
> For thumbnail images, we recommend using **1200x600px** and `.png` format.

### Adding inline blogs
1. Create a `.md` file with all the content inline following Markdown syntax
2. Place relevant images in the `website/static/images/<folder-name>` folder and refer to it in the blog

> [!NOTE]  
> You may see broken thumbnails for the blogs during `npm start` or `npm serve` because the images needs to be rendered from the main repo. This can only be tested after being merged to the main branch.

## Releases
### Adding new release
1. Create a `.mdx` file similar to `website/releases/release-0.1.0-incubating.mdx`
2. Update the [downloads](releases/downloads.mdx) file to include the new release similar to the existing releases

## Changes to the website homepage
1. The homepage is a `.html` file located at `website/static/index.html`
2. If you're making changes to the page, test it locally using `python 3 -m http.server` and visiting http://localhost:8000/ before pushing the changes.


## Maintainers
[Apache XTable™ (Incubating) Community](https://incubator.apache.org/projects/xtable.html)
