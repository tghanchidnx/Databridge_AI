/**
 * DataBridge AI - Excel Add-in Commands
 *
 * Ribbon button command handlers for quick actions
 */

/* global Office */

Office.onReady(() => {
  // Commands are ready
});

/**
 * Map Hierarchy command - opens taskpane with hierarchy mapper focused
 */
function mapHierarchy(event: Office.AddinCommands.Event) {
  // Store command context for the taskpane
  sessionStorage.setItem('databridge_command', 'mapHierarchy');

  // Show taskpane
  Office.addin.showAsTaskpane();

  event.completed();
}

/**
 * Reconcile Sheets command - opens taskpane with reconciler focused
 */
function reconcileSheets(event: Office.AddinCommands.Event) {
  // Store command context for the taskpane
  sessionStorage.setItem('databridge_command', 'reconcileSheets');

  // Show taskpane
  Office.addin.showAsTaskpane();

  event.completed();
}

/**
 * Profile Data command - profiles selected data range
 */
function profileData(event: Office.AddinCommands.Event) {
  sessionStorage.setItem('databridge_command', 'profileData');
  Office.addin.showAsTaskpane();
  event.completed();
}

// Register commands globally for Office.js
(globalThis as any).mapHierarchy = mapHierarchy;
(globalThis as any).reconcileSheets = reconcileSheets;
(globalThis as any).profileData = profileData;
