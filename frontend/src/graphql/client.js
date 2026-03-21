import { ApolloClient, InMemoryCache, HttpLink } from '@apollo/client';

// Use an explicit backend base URL to ensure the frontend talks directly to the backend.
// Prefer VITE_API_URL when provided; otherwise, default to loopback http://127.0.0.1:8005.
const API_BASE = import.meta.env.VITE_API_URL || 'http://127.0.0.1:8005';
const GRAPHQL_URL = `${API_BASE.replace(/\/+$/, '')}/graphql`;

export const apolloClient = new ApolloClient({
  link: new HttpLink({ uri: GRAPHQL_URL }),
  cache: new InMemoryCache(),
});