"""DataShield Constants - Synthetic data pools and regex patterns.

Built-in pools for the synthetic_substitution strategy. Each pool provides
deterministic lookup values indexed via HMAC for consistent mapping.
"""

# =============================================================================
# Synthetic Data Pools
# =============================================================================

SYNTHETIC_POOLS = {
    "company_names": [
        "Vertex Industries", "Apex Dynamics", "Cascade Systems", "Pinnacle Solutions",
        "Horizon Analytics", "Summit Technologies", "Vanguard Corp", "Nexus Partners",
        "Meridian Group", "Atlas Enterprises", "Zenith Holdings", "Prism Innovations",
        "Quantum Resources", "Eclipse Trading", "Fortis Capital", "Nova Logistics",
        "Sterling Ventures", "Luminary Digital", "Citadel Networks", "Cobalt Services",
        "Ironclad Industries", "Sapphire Global", "Titan Manufacturing", "Orion Labs",
        "Phoenix Consulting", "Evergreen Supply", "Beacon Financial", "Granite Corp",
        "Silver Creek LLC", "Redwood Systems", "Azure Solutions", "Platinum Partners",
        "Diamond Edge", "Crystal Technologies", "Emerald Analytics", "Obsidian Group",
        "Coral Dynamics", "Ivory Research", "Amber Holdings", "Jade Enterprises",
        "Opal Ventures", "Onyx Capital", "Pearl Digital", "Ruby Innovations",
        "Topaz Solutions", "Aqua Logistics", "Bronze Trading", "Cedar Networks",
        "Elm Financial", "Birch Consulting", "Maple Industries", "Oak Technologies",
        "Pine Resources", "Willow Systems", "Aspen Group", "Cypress Corp",
        "Hazel Partners", "Laurel Holdings", "Magnolia Labs", "Sequoia Digital",
        "Acacia Services", "Poplar Analytics", "Spruce Capital", "Teak Manufacturing",
        "Alder Ventures", "Banyan Supply", "Catalpa Trading", "Dogwood Enterprises",
        "Ficus Global", "Ginkgo Solutions", "Hickory Networks", "Juniper Research",
        "Linden Financial", "Mahogany Corp", "Nutmeg Dynamics", "Olive Industries",
        "Palmetto Technologies", "Quince Logistics", "Rosewood Innovations", "Sycamore Group",
        "Tamarind Holdings", "Walnut Labs", "Yew Digital", "Zelkova Services",
        "Alpine Industries", "Basin Technologies", "Crest Dynamics", "Delta Solutions",
        "Echo Partners", "Fjord Analytics", "Glen Systems", "Harbor Corp",
        "Isle Ventures", "Jetty Capital", "Knoll Manufacturing", "Lagoon Trading",
        "Mesa Enterprises", "Narrows Group", "Oasis Holdings", "Peak Resources",
        "Ridge Consulting", "Strait Labs", "Terrace Digital", "Valley Logistics",
    ],

    "person_names": [
        "Alex Morgan", "Jordan Chen", "Sam Rivera", "Taylor Kim", "Casey Johnson",
        "Morgan Lee", "Jamie Park", "Riley Thompson", "Quinn Williams", "Avery Brown",
        "Drew Anderson", "Blake Martinez", "Cameron White", "Dakota Harris", "Emery Clark",
        "Finley Lewis", "Harper Robinson", "Hayden Walker", "Jordan Hall", "Kendall Young",
        "Logan Allen", "Mason King", "Noel Wright", "Parker Scott", "Peyton Hill",
        "Reagan Green", "Rowan Adams", "Sage Baker", "Shannon Nelson", "Sydney Carter",
        "Tatum Mitchell", "Devon Roberts", "Elliot Turner", "Francis Phillips", "Glenn Campbell",
        "Haven Parker", "Ira Evans", "Jesse Edwards", "Kit Collins", "Lane Stewart",
        "Monroe Sanchez", "Nash Morris", "Oakley Rogers", "Perry Reed", "Robin Cook",
        "Shea Morgan", "Tanner Bailey", "Val Rivera", "Whitney Bell", "Wren Murphy",
        "Addison Cooper", "Bailey Richardson", "Charlie Cox", "Devin Howard", "Eden Ward",
        "Frankie Torres", "Gray Peterson", "Harley Gray", "Indigo Ramirez", "Jules James",
        "Kerry Watson", "Lux Brooks", "Micah Kelly", "Nico Sanders", "Onyx Price",
        "Phoenix Bennett", "Ray Wood", "Scout Barnes", "Terry Ross", "Uri Henderson",
        "Vesper Coleman", "Winter Jenkins", "Xen Perry", "Yael Powell", "Zion Long",
        "Ainsley Patterson", "Blair Hughes", "Corin Flores", "Dale Washington", "Ellis Butler",
        "Flynn Simmons", "Greer Foster", "Holiday Gonzalez", "Ivory Bryant", "Jude Alexander",
        "Kai Russell", "Lake Griffin", "Merit Diaz", "Noble Hayes", "Ocean Myers",
        "Pax Ford", "Quincy Hamilton", "River Graham", "Shiloh Sullivan", "True Wallace",
    ],

    "cities": [
        "Northfield", "Westbrook", "Clearwater", "Stonebridge", "Fairhaven",
        "Brookdale", "Ridgemont", "Lakewood", "Cedarville", "Pinecrest",
        "Silverton", "Maplewood", "Oakridge", "Willowdale", "Ashford",
        "Bridgeport", "Crestview", "Dunmore", "Eastfield", "Foxborough",
        "Glendale", "Hillcrest", "Irondale", "Jaspertown", "Kensington",
        "Lakeshore", "Millbrook", "Newbury", "Oceanview", "Parkville",
        "Queensbury", "Riverside", "Springfield", "Thornton", "Unionville",
        "Valleyford", "Windham", "Yarmouth", "Ashland", "Belmont",
        "Canterbury", "Dartmouth", "Edgewater", "Fairfield", "Greenwich",
        "Hartford", "Ivydale", "Jefferson", "Kingston", "Lancaster",
        "Montrose", "Norwood", "Oxford", "Plymouth", "Quincy",
        "Richmond", "Stafford", "Trenton", "Upton", "Vernon",
        "Wellington", "Exeter", "Yorktown", "Zenith Point", "Bayfield",
        "Clarksburg", "Dover", "Elmhurst", "Franklin", "Georgetown",
        "Hampton", "Irving", "Jacksonville", "Kingsport", "Lincoln",
    ],

    "regions": [
        "Region Alpha", "Region Beta", "Region Gamma", "Region Delta",
        "Zone North", "Zone South", "Zone East", "Zone West",
        "Zone Central", "Zone Pacific", "Zone Atlantic", "Zone Mountain",
        "Sector 1", "Sector 2", "Sector 3", "Sector 4", "Sector 5",
        "Sector 6", "Sector 7", "Sector 8", "Sector 9", "Sector 10",
        "Division A", "Division B", "Division C", "Division D",
        "District Prime", "District Nova", "District Core", "District Edge",
        "Territory North", "Territory South", "Territory East", "Territory West",
        "Area Blue", "Area Green", "Area Red", "Area Gold",
        "Quadrant I", "Quadrant II", "Quadrant III", "Quadrant IV",
        "Hub Central", "Hub Coastal", "Hub Mountain", "Hub Valley",
        "Cluster Alpha", "Cluster Beta", "Cluster Gamma", "Cluster Omega",
    ],

    "department_names": [
        "Operations Group A", "Division Theta", "Unit 12", "Branch Office Delta",
        "Marketing Division", "Engineering Services", "Finance Operations",
        "Human Resources Group", "Legal Affairs", "Information Technology",
        "Research Division", "Supply Chain Unit", "Quality Assurance",
        "Customer Relations", "Product Development", "Strategic Planning",
        "Internal Audit", "Compliance Unit", "Risk Management",
        "Corporate Services", "Data Analytics", "Digital Innovation",
        "Facilities Management", "Global Operations", "Investment Services",
        "Knowledge Management", "Logistics Center", "Manufacturing Unit",
        "Network Operations", "Procurement Services", "Revenue Operations",
        "Security Division", "Training Center", "Business Intelligence",
        "Change Management", "Design Studio", "Enterprise Solutions",
        "Field Operations", "Growth Team", "Integration Services",
        "Joint Ventures", "Key Accounts", "Learning Development",
        "Media Relations", "New Markets", "Outreach Programs",
        "Partner Relations", "Resource Planning", "Service Delivery",
        "Technical Support", "Vendor Management", "Workforce Planning",
    ],

    "product_names": [
        "Widget Pro", "Component X-47", "Module Sigma", "Platform Edge",
        "System Alpha", "Device Nova", "Engine Apex", "Suite Premium",
        "Interface 3000", "Controller Plus", "Adapter Prime", "Sensor Matrix",
        "Gateway Ultra", "Router Flex", "Switch Core", "Bridge Connect",
        "Scanner Elite", "Monitor Vision", "Tracker Pulse", "Analyzer Deep",
        "Compiler Fast", "Optimizer Max", "Builder Smart", "Mapper Pro",
        "Resolver Quick", "Validator Sure", "Converter Flow", "Extractor Lite",
        "Generator Power", "Processor Rapid", "Formatter Clean", "Renderer Sharp",
        "Aggregator Plus", "Distributor Net", "Balancer Equi", "Scheduler On",
        "Dispatcher Route", "Allocator Fair", "Synchronizer Live", "Replicator Copy",
        "Archiver Deep", "Indexer Fast", "Cacher Hot", "Buffer Stream",
        "Encoder Secure", "Decoder Open", "Translator Multi", "Parser Smart",
        "Linker Connect", "Merger Unified", "Splitter Divide", "Filter Pure",
    ],

    "country_names": [
        "Aurelia", "Belvista", "Cordania", "Draconia", "Eldara",
        "Florencia", "Galthor", "Havenia", "Istrana", "Jovelia",
        "Kaldoria", "Lumaria", "Montessa", "Novara", "Orinthos",
        "Paladora", "Quintara", "Rosandria", "Silvandor", "Thaleria",
        "Umbrava", "Valdonia", "Westaria", "Xanthoria", "Yonderia",
        "Zenithia", "Aetheron", "Borealis", "Celestia", "Dreamoria",
        "Evandor", "Fontaine", "Glaceria", "Halcyon", "Infinara",
        "Jubilant", "Kronheim", "Luxandra", "Mythoria", "Northgate",
        "Oceanis", "Prismara", "Quantis", "Riverdell", "Starfall",
    ],
}

# =============================================================================
# Column Name Classification Patterns
# =============================================================================

COLUMN_NAME_PATTERNS = {
    # Measures - amounts, quantities, rates
    "measure": [
        r"amount", r"total", r"sum", r"balance", r"price", r"cost",
        r"revenue", r"profit", r"loss", r"rate", r"quantity", r"qty",
        r"\bcount\b", r"_count$", r"^count_", r"weight", r"volume",
        r"percent", r"ratio", r"fee", r"tax", r"discount", r"margin",
        r"budget", r"salary", r"wage", r"compensation", r"pay_rate",
    ],
    # Fact dimensions - invoice #, PO #, agreement codes
    "fact_dimension": [
        r"invoice", r"purchase_order", r"po_num", r"agreement",
        r"contract", r"order_num", r"transaction", r"batch",
        r"receipt", r"voucher", r"ticket", r"reference",
        r"confirmation", r"tracking", r"shipment",
    ],
    # Descriptive - names, labels
    "descriptive": [
        r"name$", r"_name$", r"description", r"label", r"title",
        r"comment", r"note", r"remark", r"memo",
        r"vendor_name", r"customer_name", r"company_name",
        r"employee_name", r"first_name", r"last_name", r"full_name",
    ],
    # Geographic
    "geographic": [
        r"country", r"city", r"state", r"region", r"address",
        r"street", r"zip", r"postal", r"province", r"territory",
        r"location", r"latitude", r"longitude", r"geo",
    ],
    # Temporal
    "temporal": [
        r"date", r"time", r"timestamp", r"created_at", r"updated_at",
        r"period", r"year", r"month", r"day", r"quarter",
        r"effective_date", r"expiry_date", r"start_date", r"end_date",
    ],
    # Identifiers - PKs, FKs
    "identifier": [
        r"_id$", r"_key$", r"_pk$", r"_fk$", r"surrogate",
        r"^id$", r"^key$", r"uuid", r"guid",
    ],
    # Codes - GL accounts, cost centers
    "code": [
        r"_code$", r"account", r"cost_center", r"gl_",
        r"segment", r"department_code", r"product_code",
        r"category_code", r"class_code", r"type_code",
    ],
    # Sensitive PII
    "sensitive_pii": [
        r"ssn", r"social_security", r"email", r"e_mail", r"phone",
        r"mobile", r"cell", r"fax", r"credit_card", r"card_number",
        r"ccn", r"pan", r"cvv", r"cvc", r"password", r"passwd",
        r"token", r"api_key", r"secret", r"date_of_birth", r"dob",
        r"birth_date", r"medical", r"diagnosis", r"patient",
    ],
    # Safe - flags, statuses
    "safe": [
        r"status", r"flag", r"is_", r"has_", r"type$",
        r"currency", r"uom", r"unit", r"active",
        r"enabled", r"deleted", r"version",
    ],
}

# =============================================================================
# Value-level PII Patterns (regex for detecting PII in values)
# =============================================================================

VALUE_PII_PATTERNS = {
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    "phone_us": r"^\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$",
    "ssn": r"^\d{3}-\d{2}-\d{4}$",
    "credit_card": r"^\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}$",
    "ip_address": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
    "date_iso": r"^\d{4}-\d{2}-\d{2}",
}
