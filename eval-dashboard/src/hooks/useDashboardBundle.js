import { useEffect, useState } from 'react';

export function useManifest() {
  const [manifest, setManifest] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch('./runs/manifest.json')
      .then((r) => (r.ok ? r.json() : []))
      .then((data) => setManifest(Array.isArray(data) ? data : []))
      .catch((e) => setError(e.message));
  }, []);

  return { manifest, error };
}

export function useDashboardBundle(runId) {
  const [bundle, setBundle] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!runId) {
      setBundle(null);
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    fetch(`./runs/${runId}.json`)
      .then((r) => {
        if (!r.ok) throw new Error(`Failed to load run ${runId}`);
        return r.json();
      })
      .then((data) => {
        setBundle(data);
        setLoading(false);
      })
      .catch((e) => {
        setError(e.message);
        setLoading(false);
      });
  }, [runId]);

  return { bundle, loading, error };
}

export function fmtPct(v, digits = 1) {
  if (v == null || Number.isNaN(v)) return '—';
  return `${(Number(v) * 100).toFixed(digits)}%`;
}

export function fmtNum(v, digits = 2) {
  if (v == null || Number.isNaN(v)) return '—';
  return Number(v).toFixed(digits);
}

export function fmtUsd(v) {
  if (v == null || Number.isNaN(v)) return '—';
  return `$${Number(v).toFixed(4)}`;
}
