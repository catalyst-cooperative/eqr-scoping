CREATE TYPE exchange_brokerage_service_enum AS ENUM (
    'BROKER',
    'ICE',
    'NODAL',
    'NYMEX'
);

CREATE TYPE type_of_rate_enum AS ENUM (
    'ELECTRIC INDEX',
    'FIXED',
    'FORMULA',
    'RTO/ISO'
);

-- These do not look like they'll be fun.
CREATE TYPE time_zone_enum AS ENUM (
    'CD',
    'CP',
    'CS',
    'ED',
    'EP',
    'EPT',
    'ES',
    'EST',
    'MD',
    'MP',
    'MS',
    'PD',
    'PP',
    'PS'
);

CREATE TYPE class_name_enum AS ENUM (
    'BA',
    'F',
    'NF',
    'UP'
);

CREATE TYPE term_name_enum AS ENUM (
    'LT',
    'ST'
);

CREATE TYPE increment_name_enum AS ENUM (
    '15',
    '5',
    'D',
    'H',
    'M',
    'W',
    'Y'
);

CREATE TYPE increment_peaking_name_enum AS ENUM (
    'FP',
    'OP',
    'P'
);

CREATE TYPE product_name_enum AS ENUM (
    'BLACK START SERVICE',
    'BOOKED OUT POWER',
    'CAPACITY',
    'CUSTOMER CHARGE',
    'ENERGY',
    'ENERGY IMBALANCE',
    'EXCHANGE',
    'FUEL CHARGE',
    'GENERATOR IMBALANCE',
    'GRANDFATHERED BUNDLED',
    'NEGOTIATED RATE TRANSMISSION',
    'OTHER',
    'PRIMARY FREQUENCY RESPONSE',
    'REACTIVE SUPPLY & VOLTAGE CONTROL',
    'REAL POWER TRANSMISSION LOSS',
    'REGULATION & FREQUENCY RESPONSE',
    'REQUIREMENTS SERVICE',
    'SCHEDULE SYSTEM CONTROL & DISPATCH',
    'SPINNING RESERVE',
    'SUPPLEMENTAL RESERVE',
    'TOLLING ENERGY',
    'UPLIFT'
);

-- Do any of these need to be consolidated? e.g. are KW-DAY and $/KW-DAY the same?
-- Would we want to convert to uniform units for easy comparison?
CREATE TYPE rate_units_enum AS ENUM (
    '$/KVA',
    '$/KVR',
    '$/KW',
    '$/KW-DAY',
    '$/KW-MO',
    '$/KW-YR',
    '$/KWH',
    '$/MVAR-YR',
    '$/MW',
    '$/MW-DAY',
    '$/MW-MO',
    '$/MW-YR',
    '$/MWH',
    '$/RKVA',
    'CENTS/KWH',
    'FLAT RATE'
);
