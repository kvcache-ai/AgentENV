// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

(() => {
    const mermaidScriptUrl = 'https://cdn.jsdelivr.net/npm/mermaid@11.12.0/dist/mermaid.min.js';
    const mermaidScriptIntegrity = 'sha384-o+g/BxPwhi0C3RK7oQBxQuNimeafQ3GE/ST4iT2BxVI4Wzt60SH4pq9iXVYujjaS';
    const darkThemes = ['ayu', 'navy', 'coal'];
    const lightThemes = ['light', 'rust'];

    const classList = document.getElementsByTagName('html')[0].classList;

    let lastThemeWasLight = true;
    for (const cssClass of classList) {
        if (darkThemes.includes(cssClass)) {
            lastThemeWasLight = false;
            break;
        }
    }

    const initializeMermaid = () => {
        const theme = lastThemeWasLight ? 'default' : 'dark';
        window.mermaid.initialize({ startOnLoad: false, theme });
        window.mermaid.run().catch((error) => {
            console.error('Failed to render Mermaid diagrams', error);
        });
    };

    const mermaidScript = document.createElement('script');
    mermaidScript.src = mermaidScriptUrl;
    mermaidScript.integrity = mermaidScriptIntegrity;
    mermaidScript.crossOrigin = 'anonymous';
    mermaidScript.onload = initializeMermaid;
    mermaidScript.onerror = () => {
        console.error(`Failed to load Mermaid from ${mermaidScriptUrl}`);
    };
    document.head.appendChild(mermaidScript);

    // Simplest way to make mermaid re-render the diagrams in the new theme is via refreshing the page

    for (const darkTheme of darkThemes) {
        const themeButton = document.getElementById(`mdbook-theme-${darkTheme}`);
        themeButton?.addEventListener('click', () => {
            if (lastThemeWasLight) {
                window.location.reload();
            }
        });
    }

    for (const lightTheme of lightThemes) {
        const themeButton = document.getElementById(`mdbook-theme-${lightTheme}`);
        themeButton?.addEventListener('click', () => {
            if (!lastThemeWasLight) {
                window.location.reload();
            }
        });
    }
})();
