<style>
  /* Stoplight uses its own multi-column navigation, so let it use the full
     mdBook content area instead of the default narrow reading column. */
  html,
  body,
  .page-wrapper,
  .page {
    height: 100% !important;
    overflow: hidden !important;
  }
  .content {
    height: calc(100vh - 50px) !important;
    overflow: hidden !important;
    padding: 0 !important;
  }
  .content main {
    width: 100% !important;
    max-width: none !important;
    margin: 0 !important;
    padding: 0 !important;
  }
  /* This page has navigation inside Stoplight; mdBook chapter arrows overlap it. */
  .nav-chapters,
  .mobile-nav-chapters {
    display: none !important;
  }
</style>
<iframe
  id="stoplight-elements-frame"
  src="../stoplight-elements.html#/paths/health/get"
  title="AgentENV API Reference rendered with Stoplight Elements"
  style="width: 100%; height: calc(100vh - 50px); border: none; display: block;">
</iframe>
