import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// Relative paths let the built site run from any host or sub-folder.
export default defineConfig({ plugins: [react()], base: './' })
