// FORCE TEST: if you don't see "BOOT TEST" below, this is not the project being served.
document.getElementById("root").textContent = "BOOT TEST";
// now mount React:
import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App.jsx";
createRoot(document.getElementById("root")).render(<App />);
