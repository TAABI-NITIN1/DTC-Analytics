const customerAliasByKey = new Map();
const customerOriginalByKey = new Map();
const vehicleAliasByKey = new Map();
const vehicleOriginalByKey = new Map();

const normalizeValue = (value) => {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value).trim();
};

const normalizeKey = (value) => normalizeValue(value).toLowerCase();

const assignAlias = (aliasMap, originalMap, rawValue, prefix) => {
  const original = normalizeValue(rawValue);
  if (!original) {
    return '';
  }

  const key = normalizeKey(original);
  if (!aliasMap.has(key)) {
    const alias = `${prefix}${aliasMap.size + 1}`;
    aliasMap.set(key, alias);
    originalMap.set(key, original);
  }

  return aliasMap.get(key) || '';
};

const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const applyAliasReplacements = (text, originalMap, aliasMap) => {
  let masked = text;
  const originals = Array.from(originalMap.entries())
    .map(([key, original]) => ({ key, original, alias: aliasMap.get(key) || '' }))
    .filter((entry) => entry.original && entry.alias)
    .sort((a, b) => b.original.length - a.original.length);

  for (const entry of originals) {
    const pattern = new RegExp(escapeRegExp(entry.original), 'gi');
    masked = masked.replace(pattern, entry.alias);
  }

  return masked;
};

const CUSTOMER_KEYS = ['customerName', 'customer_name'];
const VEHICLE_KEYS = ['uniqueid', 'vehicleNumber', 'vehicle_number'];

const registerAliasesFromObject = (obj) => {
  if (!obj || typeof obj !== 'object') {
    return;
  }

  for (const key of CUSTOMER_KEYS) {
    if (key in obj) {
      aliasCustomerName(obj[key]);
    }
  }

  for (const key of VEHICLE_KEYS) {
    if (key in obj) {
      aliasVehicle(obj[key]);
    }
  }
};

export function aliasCustomerName(value) {
  return assignAlias(customerAliasByKey, customerOriginalByKey, value, 'c');
}

export function aliasVehicle(value) {
  return assignAlias(vehicleAliasByKey, vehicleOriginalByKey, value, 'vehicle');
}

export function aliasVehicleFromRow(row) {
  if (!row || typeof row !== 'object') {
    return '';
  }

  const identifier = row.uniqueid || row.vehicleNumber || row.vehicle_number || row.id || '';
  return aliasVehicle(identifier);
}

export function registerAliasesFromData(value) {
  if (Array.isArray(value)) {
    value.forEach(registerAliasesFromData);
    return;
  }

  if (!value || typeof value !== 'object') {
    return;
  }

  registerAliasesFromObject(value);
  Object.values(value).forEach(registerAliasesFromData);
}

export function maskSensitiveText(value) {
  if (value === null || value === undefined) {
    return value;
  }

  let masked = String(value);
  masked = applyAliasReplacements(masked, customerOriginalByKey, customerAliasByKey);
  masked = applyAliasReplacements(masked, vehicleOriginalByKey, vehicleAliasByKey);
  return masked;
}

export function maskSensitiveObject(value) {
  if (Array.isArray(value)) {
    return value.map(maskSensitiveObject);
  }

  if (!value || typeof value !== 'object') {
    if (typeof value === 'string') {
      return maskSensitiveText(value);
    }
    return value;
  }

  const out = {};
  for (const [key, nested] of Object.entries(value)) {
    out[key] = maskSensitiveObject(nested);
  }
  return out;
}
