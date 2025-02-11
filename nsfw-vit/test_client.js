// ==UserScript==
// @name        Show NSFW detection results for bsky.app
// @namespace   gudzpoz
// @match       https://bsky.app/*
// @grant       none
// @version     1.0
// @author      gudzpoz
// @description 2/12/2025, 3:09:28 AM
// ==/UserScript==

[
  `.nsfw-loading::after {
    content: "NSFW: loading";
  }`, `.nsfw-info, .nsfw-loading::after {
    display: block;
    position: absolute;
    z-index: 10;
    background: #fffa;
    backdrop-filter: blur(5px);
  }`,
].forEach((rule) => {
  document.styleSheets[0].insertRule(rule);
});

(() => {
  const observer = new IntersectionObserver(async (entries, obs) => {
    /**
     * @type {Element[]}
     */
    const images = [];
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        obs.unobserve(entry.target);
        images.push(entry.target);
        entry.target.setAttribute('data-nsfw', 'loading');
        entry.target.parentElement.classList.add('nsfw-loading');
      }
    });
    if (images.length > 0) {
      const res = await fetch('http://localhost:5000/nsfw', {
        method: 'POST',
        body: images.map((ele) => ele.src).join('\n'),
      });
      /**
       * @type {any[]}
       */
      const data = await res.json();
      data.forEach((item, i) => {
        let msg;
        if (typeof item === 'string') {
          msg = item;
        } else {
          const { nsfw, sfw } = item;
          const msgs = [`NSFW: ${nsfw.toFixed(2)}`, `SFW: ${sfw.toFixed(2)}`];
          const i = nsfw > sfw ? 0 : 1;
          msgs[i] = `<b>${msgs[i]}</b>`;
          msg = msgs.join(', ');
        }
        const infoBox = document.createElement('div');
        infoBox.classList.add('nsfw-info');
        infoBox.innerHTML = msg;
        images[i].parentElement.insertBefore(infoBox, images[i]);
        images[i].parentElement.classList.remove('nsfw-loading');
      });
    }
  }, {
    threshold: 0.5,
  });

  const detect = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => {
      mutation.addedNodes.forEach((node) => {
        if (node.nodeType === Node.ELEMENT_NODE) {
          const imgs = node.querySelectorAll('img[src^="https://cdn.bsky.app/img/feed_thumbnail"]');
          imgs.forEach((img) => {
            if (img.getAttribute('data-nsfw') === null) {
              observer.observe(img);
            }
          });
        }
      });
    });
  });
  detect.observe(document.body, {
    childList: true,
    subtree: true,
  });
})();
