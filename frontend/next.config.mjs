import path from 'path';

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  pageExtensions: ['ts', 'tsx', 'js', 'jsx'],
  outputFileTracingRoot: path.resolve('./'),
  // Evita que isomorphic-dompurify intente cargar browser/default-stylesheet.css en SSR
  serverExternalPackages: ['isomorphic-dompurify'],
}

export default nextConfig
