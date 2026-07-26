import React from 'react';
import clsx from 'clsx';
import {useBlogPost} from '@docusaurus/plugin-content-blog/client';
import {useDateTimeFormat} from '@docusaurus/theme-common/internal';
import styles from './styles.module.css';

function DateTime({date, formattedDate}) {
  return (
    <time dateTime={date} itemProp="datePublished">
      {formattedDate}
    </time>
  );
}

export default function BlogPostItemHeaderInfo({className}) {
  const {metadata} = useBlogPost();
  const {date} = metadata;
  // Docusaurus 3.10 dropped `metadata.formattedDate`, so format the date here
  // the way theme-classic does. Reading time is deliberately not shown.
  const dateTimeFormat = useDateTimeFormat({
    day: 'numeric',
    month: 'long',
    year: 'numeric',
    timeZone: 'UTC',
  });
  return (
    <div className={clsx(styles.container, 'margin-vert--md', className)}>
      <DateTime date={date} formattedDate={dateTimeFormat.format(new Date(date))} />
    </div>
  );
}
