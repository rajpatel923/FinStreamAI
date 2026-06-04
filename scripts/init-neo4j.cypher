// ============================================================
// Neo4j Schema Initialization
// Run against: finstreami-neo4j (port 7687)
// ============================================================

// ─── Constraints ─────────────────────────────────────────────
CREATE CONSTRAINT company_symbol IF NOT EXISTS
    FOR (c:Company) REQUIRE c.symbol IS UNIQUE;

CREATE CONSTRAINT event_id IF NOT EXISTS
    FOR (e:Event) REQUIRE e.event_id IS UNIQUE;

CREATE CONSTRAINT article_id IF NOT EXISTS
    FOR (a:Article) REQUIRE a.article_id IS UNIQUE;

CREATE CONSTRAINT sector_name IF NOT EXISTS
    FOR (s:Sector) REQUIRE s.name IS UNIQUE;

// ─── Indexes ─────────────────────────────────────────────────
CREATE INDEX company_sector IF NOT EXISTS
    FOR (c:Company) ON (c.sector);

CREATE INDEX company_name IF NOT EXISTS
    FOR (c:Company) ON (c.name);

CREATE INDEX event_type IF NOT EXISTS
    FOR (e:Event) ON (e.event_type);

// ─── Seed Sectors (S&P 500 GICS) ─────────────────────────────
MERGE (:Sector {name: 'Technology'});
MERGE (:Sector {name: 'Finance'});
MERGE (:Sector {name: 'Healthcare'});
MERGE (:Sector {name: 'Energy'});
MERGE (:Sector {name: 'Consumer Discretionary'});
MERGE (:Sector {name: 'Consumer Staples'});
MERGE (:Sector {name: 'Industrials'});
MERGE (:Sector {name: 'Materials'});
MERGE (:Sector {name: 'Real Estate'});
MERGE (:Sector {name: 'Utilities'});
MERGE (:Sector {name: 'Communication Services'});

// ─── Seed Example Companies ──────────────────────────────────
MERGE (c:Company {symbol: 'AAPL'})
SET c.name = 'Apple Inc.', c.sector = 'Technology'
WITH c
MATCH (s:Sector {name: 'Technology'})
MERGE (c)-[:PART_OF]->(s);

MERGE (c:Company {symbol: 'MSFT'})
SET c.name = 'Microsoft Corporation', c.sector = 'Technology'
WITH c
MATCH (s:Sector {name: 'Technology'})
MERGE (c)-[:PART_OF]->(s);

MERGE (c:Company {symbol: 'GOOGL'})
SET c.name = 'Alphabet Inc.', c.sector = 'Technology'
WITH c
MATCH (s:Sector {name: 'Technology'})
MERGE (c)-[:PART_OF]->(s);

MERGE (c:Company {symbol: 'AMZN'})
SET c.name = 'Amazon.com Inc.', c.sector = 'Consumer Discretionary'
WITH c
MATCH (s:Sector {name: 'Consumer Discretionary'})
MERGE (c)-[:PART_OF]->(s);

MERGE (c:Company {symbol: 'TSLA'})
SET c.name = 'Tesla Inc.', c.sector = 'Consumer Discretionary'
WITH c
MATCH (s:Sector {name: 'Consumer Discretionary'})
MERGE (c)-[:PART_OF]->(s);

MERGE (c:Company {symbol: 'META'})
SET c.name = 'Meta Platforms Inc.', c.sector = 'Communication Services'
WITH c
MATCH (s:Sector {name: 'Communication Services'})
MERGE (c)-[:PART_OF]->(s);

MERGE (c:Company {symbol: 'NVDA'})
SET c.name = 'NVIDIA Corporation', c.sector = 'Technology'
WITH c
MATCH (s:Sector {name: 'Technology'})
MERGE (c)-[:PART_OF]->(s);

MERGE (c:Company {symbol: 'JPM'})
SET c.name = 'JPMorgan Chase & Co.', c.sector = 'Finance'
WITH c
MATCH (s:Sector {name: 'Finance'})
MERGE (c)-[:PART_OF]->(s);
