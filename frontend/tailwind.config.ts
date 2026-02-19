import type { Config } from 'tailwindcss'

export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        ink: '#1f2d33',
        muted: '#5f7178',
        accent: '#17444c',
        panel: 'rgba(255,255,255,0.9)',
      },
      boxShadow: {
        panel: '0 8px 24px rgba(20,30,40,0.08)',
      },
      backgroundImage: {
        aura: 'radial-gradient(1100px 560px at 0 -18%, #f8d3a8, transparent 58%), radial-gradient(900px 460px at 100% -26%, #dff3eb, transparent 58%), linear-gradient(120deg, #f6f0e4, #eef7f3)',
      },
    },
  },
  plugins: [],
} satisfies Config
