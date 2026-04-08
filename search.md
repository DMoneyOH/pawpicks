---
layout: default
title: Search Reviews
permalink: /search/
---

<section class="about-page" style="max-width:780px;margin:3rem auto;padding:0 1.5rem;">

  <h1 style="font-family:'Fredoka',sans-serif;color:var(--teal);font-size:clamp(1.8rem,4vw,2.6rem);margin-bottom:0.5rem;">Search Reviews</h1>
  <p style="color:var(--coral);font-weight:700;font-size:1.1rem;margin-bottom:2rem;">Find the perfect product review for your dog or cat.</p>

  <div style="position:relative;margin-bottom:0.75rem;">
    <input
      type="text"
      id="search-input"
      placeholder='Try "dog bed" or "flea prevention"...'
      autocomplete="off"
      autofocus
      style="width:100%;box-sizing:border-box;padding:0.9rem 3.25rem 0.9rem 1.25rem;font-family:'Nunito',sans-serif;font-size:1.05rem;border:2.5px solid var(--border);border-radius:50px;background:#fff;color:var(--ink);box-shadow:0 2px 8px rgba(0,0,0,0.08);outline:none;transition:border-color 0.2s;"
      onfocus="this.style.borderColor='var(--coral)'"
      onblur="this.style.borderColor='var(--border)'"
    />
    <span style="position:absolute;right:1.1rem;top:50%;transform:translateY(-50%);font-size:1.15rem;pointer-events:none;">🔍</span>
  </div>

  <p id="search-status" style="color:#888;font-size:0.95rem;min-height:1.4em;margin:0.5rem 0 1rem;"></p>
  <ul id="search-results" style="list-style:none;padding:0;margin:0;display:flex;flex-direction:column;gap:0.85rem;"></ul>

  <div style="margin-top:2.5rem;">
    <a href="/" style="display:inline-block;background:var(--coral);color:#fff;font-family:'Fredoka',sans-serif;font-size:1.1rem;padding:0.75rem 2rem;border-radius:50px;text-decoration:none;">Browse All Reviews &#8594;</a>
  </div>

</section>

<script src="https://cdnjs.cloudflare.com/ajax/libs/lunr.js/2.3.9/lunr.min.js"></script>
<script>
(function () {
  var idx, docs = [];

  // Species keyword maps
  var DOG_WORDS  = ['dog','dogs','puppy','puppies','canine','pup','pups'];
  var CAT_WORDS  = ['cat','cats','kitten','kittens','feline'];

  function detectSpecies(query) {
    var q = query.toLowerCase().split(/\s+/);
    var hasDog = q.some(function(w){ return DOG_WORDS.indexOf(w) > -1; });
    var hasCat = q.some(function(w){ return CAT_WORDS.indexOf(w) > -1; });
    if (hasDog && !hasCat) return 'dog';
    if (hasCat && !hasDog) return 'cat';
    return null; // no filter
  }

  fetch('{{ site.baseurl }}/search.json')
    .then(function(r){ return r.json(); })
    .then(function(data){
      docs = data;
      idx = lunr(function () {
        this.ref('id');
        this.field('title',      { boost: 10 });
        this.field('tags',       { boost: 5  });
        this.field('categories', { boost: 3  });
        this.field('excerpt');
        data.forEach(function(d){ this.add(d); }, this);
      });
      var q = new URLSearchParams(window.location.search).get('q');
      if (q) { document.getElementById('search-input').value = q; runSearch(q); }
    });

  function runSearch(query) {
    var status  = document.getElementById('search-status');
    var list    = document.getElementById('search-results');
    list.innerHTML = '';
    if (!query || query.trim().length < 2) { status.textContent = ''; return; }

    var speciesFilter = detectSpecies(query);
    var raw = idx ? idx.search(query) : [];

    // Apply species pre-filter then require title/tag relevance, cap at 6
    var queryTerms = query.toLowerCase().split(/\s+/).filter(function(w){ return w.length > 2; });
    var results = raw.filter(function(r){
      var doc = docs.find(function(d){ return d.id === parseInt(r.ref); });
      if (!doc) return false;
      // Species filter
      if (speciesFilter && doc.species !== speciesFilter && doc.species !== 'both' && doc.species) return false;
      // Require at least one query term to appear in title or tags
      var titleTags = ((doc.title || '') + ' ' + (doc.tags || '')).toLowerCase();
      return queryTerms.length === 0 || queryTerms.some(function(t){ return titleTags.indexOf(t) > -1; });
    }).slice(0, 6);

    if (results.length === 0) {
      status.textContent = 'No results for "' + query + '". Try a different term.';
      return;
    }
    status.textContent = results.length + ' result' + (results.length !== 1 ? 's' : '') + ' for "' + query + '"'
      + (speciesFilter ? ' (' + speciesFilter + 's only)' : '');

    results.forEach(function(r){
      var doc = docs.find(function(d){ return d.id === parseInt(r.ref); });
      if (!doc) return;
      var emoji = doc.species === 'dog' ? '🐶' : doc.species === 'cat' ? '🐱' : '🐾';
      var li = document.createElement('li');
      li.innerHTML =
        '<a href="' + doc.url + '" style="display:flex;align-items:flex-start;gap:0.85rem;padding:1rem 1.25rem;background:#fff;border:2px solid var(--border);border-radius:16px;text-decoration:none;color:inherit;transition:box-shadow 0.2s,transform 0.2s;"'
        + ' onmouseover="this.style.boxShadow=\'0 4px 16px rgba(0,0,0,0.10)\';this.style.transform=\'translateY(-2px)\'"'
        + ' onmouseout="this.style.boxShadow=\'\';this.style.transform=\'\'">'
        + '<span style="font-size:1.6rem;flex-shrink:0;margin-top:0.1rem;">' + emoji + '</span>'
        + '<div>'
        + '<strong style="font-family:\'Fredoka\',sans-serif;font-size:1.1rem;color:var(--teal);display:block;margin-bottom:0.25rem;">' + doc.title + '</strong>'
        + '<p style="font-size:0.9rem;color:#666;margin:0;line-height:1.5;">' + doc.excerpt + '</p>'
        + '</div></a>';
      list.appendChild(li);
    });
  }

  document.getElementById('search-input').addEventListener('input', function(){
    runSearch(this.value);
  });
})();
</script>
