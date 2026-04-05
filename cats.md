---
layout: default
title: Cat Product Reviews | Happy Pet Product Reviews
description: In-depth reviews of the best cat products - litter, scratchers, feeders, carriers and more. Honest picks for every budget.
permalink: /cats/
---
<section class="posts-section">
  <div class="section-header">
    <h2 class="section-title">Cat Product Reviews</h2>
    {% assign cat_only = site.posts | where: "species", "cat" %}
    {% assign cat_both = site.posts | where: "species", "both" %}
    {% assign cat_posts = cat_only | concat: cat_both %}
    <span class="section-count">{{ cat_posts | size }} reviews</span>
  </div>
  <div class="post-grid">
    {% if cat_posts.size == 0 %}
      <div class="empty-state"><p>🐾 Cat reviews coming soon!</p></div>
    {% else %}
      {% for post in cat_posts %}
      <article class="post-card">
        <div class="card-accent"></div>
        <a href="{{ post.url | prepend: site.baseurl }}" class="card-link">
          <div class="card-top">
            <div class="card-badges">
              <span class="card-category">{{ post.categories | first | replace: "-", " " | capitalize }}</span>
              <span class="card-species species-cat">🐈 Cat</span>
            </div>
          </div>
          <h2 class="card-title">{{ post.title }}</h2>
          <p class="card-excerpt">{{ post.excerpt | strip_html | truncate: 115 }}</p>
          <div class="card-footer">
            <time class="card-date">{{ post.date | date: "%b %d, %Y" }}</time>
            <span class="card-read">Read Review →</span>
          </div>
        </a>
      </article>
      {% endfor %}
    {% endif %}
  </div>
</section>
