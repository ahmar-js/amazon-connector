// Centralized marketplace mappings for frontend
// Each marketplace entry contains:
// - code: marketplace code (e.g., 'US', 'DE')
// - id: Amazon marketplace ID
// - region: short region identifier used by SP-API calls ('na' or 'eu')
// - disabled: boolean flag; if true the UI should show the marketplace as disabled

export type Marketplace = {
  code: string;
  id: string;
  region: string;
  disabled: boolean;
  name?: string; // optional human-friendly name
};

export const MARKETPLACES: Record<string, Marketplace> = {
  US: {
    code: 'US',
    id: 'ATVPDKIKX0DER',
    region: 'na',
    disabled: false,
    name: 'United States'
  },
  CA: {
    code: 'CA',
    id: 'A2EUQ1WTGCTBG2',
    region: 'na',
    disabled: false,
    name: 'Canada'
  },
  UK: {
    code: 'UK',
    id: 'A1F83G8C2ARO7P',
    region: 'eu',
    disabled: false,
    name: 'United Kingdom'
  },
  DE: {
    code: 'DE',
    id: 'A1PA6795UKMFR9',
    region: 'eu',
    disabled: false,
    name: 'Germany'
  },
  FR: {
    code: 'FR',
    id: 'A13V1IB3VIYZZH',
    region: 'eu',
    disabled: true,
    name: 'France'
  },
  IT: {
    code: 'IT',
    id: 'APJ6JRA9NG5V4',
    region: 'eu',
    disabled: false,
    name: 'Italy'
  },
  ES: {
    code: 'ES',
    id: 'A1RKKUPIHCS9HS',
    region: 'eu',
    disabled: false,
    name: 'Spain'
  }
};

export function getAvailableMarketplaces(): Record<string, Marketplace> {
  return MARKETPLACES;
}

export function getEnabledMarketplaceCodes(): string[] {
  return Object.keys(MARKETPLACES).filter(code => !MARKETPLACES[code].disabled);
}

export function getEnabledMarketplacesArray(): Marketplace[] {
  return getEnabledMarketplaceCodes().map(code => MARKETPLACES[code]);
}

export function getMarketplaceByCode(code: string): Marketplace | undefined {
  return MARKETPLACES[code];
}

export function getMarketplaceId(code: string): string | undefined {
  return MARKETPLACES[code]?.id;
}

export default MARKETPLACES;
