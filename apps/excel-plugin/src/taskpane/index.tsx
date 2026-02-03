/**
 * DataBridge AI - Excel Add-in Entry Point
 */

import React from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';

/* global Office */

// Initialize Office.js
Office.onReady((info) => {
  if (info.host === Office.HostType.Excel) {
    console.log('DataBridge AI Excel Add-in loaded');

    // Render the React app
    const container = document.getElementById('root');
    if (container) {
      const root = createRoot(container);
      root.render(
        <React.StrictMode>
          <App />
        </React.StrictMode>
      );
    }
  }
});
