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


## Maintainers
[Apache XTable™ (Incubating) Community](https://incubator.apache.org/projects/xtable.html)
