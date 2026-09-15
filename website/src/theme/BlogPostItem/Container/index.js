import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {useLocation} from 'react-router-dom';
import {useBaseUrlUtils} from '@docusaurus/useBaseUrl';
import {useBlogPost} from '@docusaurus/plugin-content-blog/client';

export default function BlogPostItemContainer({children, className}) {
  const {
    frontMatter,
    assets,
    isBlogPostPage,
    metadata: {description, permalink},
  } = useBlogPost();
  const location = useLocation();
  const {withBaseUrl} = useBaseUrlUtils();
  const image = assets.image ?? frontMatter.image;
  const keywords = frontMatter.keywords ?? [];

  return (
    <article
      className={clsx(className, !isBlogPostPage && 'blogPostCard')}
      itemProp="blogPost"
      itemScope
      itemType="https://schema.org/BlogPosting">
      {description && <meta itemProp="description" content={description} />}
      {image && (
        <div className="col blogThumbnail" itemProp="blogThumbnail">
          {location.pathname.startsWith('/blog') ? (
            <Link itemProp="url" to={permalink}>
              <img
                src={withBaseUrl(image)}
                className="blog-image"
              />
            </Link>
          ) : null}
        </div>
      )}
      {keywords.length > 0 && (
        <meta itemProp="keywords" content={keywords.join(',')} />
      )}
      {children}
    </article>
  );
}