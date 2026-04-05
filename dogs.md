---
layout: default
title: Dog Product Reviews | Happy Pet Product Reviews
description: In-depth reviews of the best dog products - collars, harnesses, toys, beds and more. Honest picks for every budget.
permalink: /dogs/
---
<section class="posts-section">
  <div class="section-header">
    <h2 class="section-title">Dog Product Reviews</h2>
    {% assign dog_only = site.posts | where: "species", "dog" %}
    {% assign dog_both = site.posts | where: "species", "both" %}
    {% assign dog_posts = dog_only | concat: dog_both %}
    <span class="section-count">{{ dog_posts | size }} reviews</span>
  </div>
  <div class="post-grid">
    {% if dog_posts.size == 0 %}
      <div class="empty-state"><p>🐾 Dog reviews coming soon!</p></div>
    {% else %}
      {% for post in dog_posts %}
      <article class="post-card">
        <div class="card-accent"></div>
        <a href="{{ post.url | prepend: site.baseurl }}" class="card-link">
          <div class="card-top">
            <div class="card-badges">
              <span class="card-category">{{ post.categories | first | replace: "-", " " | capitalize }}</span>
              <span class="card-species species-dog">🐕 Dog</span>
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
