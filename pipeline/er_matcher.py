import re
from pipeline.normalize import normalize_name, normalize_address, normalize_city


def _get_tokens(name: str) -> set[str]:
    return set(w for w in name.split() if len(w) > 2)


def _jaccard(s1: set, s2: set) -> float:
    if not s1 or not s2:
        return 0
    return len(s1 & s2) / len(s1 | s2)


def _token_containment(query: set, candidate: set) -> float:
    if not query:
        return 0
    return len(query & candidate) / len(query)


def _address_similarity(addr1: str, addr2: str) -> float:
    a1 = normalize_address(addr1)
    a2 = normalize_address(addr2)
    if not a1 or not a2:
        return 0.0
    if a1 == a2:
        return 1.0
    if a1 in a2 or a2 in a1:
        return 0.9
    m1 = re.match(r"^(\d+)\s+(.+)", a1)
    m2 = re.match(r"^(\d+)\s+(.+)", a2)
    if m1 and m2 and m1.group(1) == m2.group(1):
        t1 = set(m1.group(2).split())
        t2 = set(m2.group(2).split())
        if t1 and t2:
            overlap = len(t1 & t2) / max(len(t1), len(t2))
            if overlap >= 0.5:
                return 0.7 + overlap * 0.2
    t1 = set(a1.split())
    t2 = set(a2.split())
    if t1 and t2:
        return len(t1 & t2) / len(t1 | t2) * 0.5
    return 0.0


def is_govt_entity(rec: dict) -> bool:
    govt_types = {
        "government", "local_government", "regional_and_state_government",
        "national_government", "indian_native_american_tribal_government",
        "council_of_governments", "authorities_and_commissions",
    }
    biz = set(rec.get("business_types", []))
    has_nonprofit = "nonprofit" in biz or "corporate_entity_tax_exempt" in biz
    has_govt = bool(biz & govt_types)
    return has_govt and not has_nonprofit


def match_recipient(rec: dict, bmf_index: dict) -> tuple[dict | None, float, str | None]:
    """Match a USASpending recipient against BMF index. Returns (match, score, method)."""
    all_names = [rec["name"]] + rec.get("alt_names", [])[:10]
    rzip = str(rec.get("zip", ""))[:5]
    rcity = normalize_city(rec.get("city", ""))
    raddr = rec.get("address", "")

    best_match = None
    best_score = 0.0
    best_method = None

    for name_variant in all_names:
        qname = normalize_name(name_variant)
        qtokens = _get_tokens(qname)
        if not qtokens:
            continue

        # Pass 1: Exact name
        candidates = bmf_index["by_exact"].get(qname, [])
        if candidates:
            for c in candidates:
                addr_sim = _address_similarity(raddr, c.get("STREET", ""))
                if c["_zip5"] == rzip:
                    return c, 1.0, f"exact+zip (addr={addr_sim:.2f})"
            for c in candidates:
                if c["_norm_city"] == rcity:
                    return c, 0.95, "exact+city"
            for c in candidates:
                addr_sim = _address_similarity(raddr, c.get("STREET", ""))
                if addr_sim >= 0.7:
                    return c, 0.90, f"exact+addr (addr={addr_sim:.2f})"
            return candidates[0], 0.82, "exact_only"

        # Pass 2: Token in same city
        for c in bmf_index["by_city"].get(rcity, []):
            j = _jaccard(qtokens, c["_tokens"])
            cont = _token_containment(qtokens, c["_tokens"])
            if j >= 0.5 or cont >= 0.75:
                addr_sim = _address_similarity(raddr, c.get("STREET", ""))
                score = j * 0.3 + cont * 0.2 + addr_sim * 0.25 + 0.1
                if addr_sim >= 0.7:
                    score += 0.1
                if score > best_score:
                    best_score = score
                    best_match = c
                    best_method = f"token+city (j={j:.2f}, addr={addr_sim:.2f})"

    # Pass 2.5: Address-only lookup for nonprofits (catches renamed orgs)
    # Only for nonprofit-classified recipients — BMF is a nonprofit registry
    # so address match is high signal. Avoids noise from for-profits at shared addresses.
    if best_score < 0.55 and raddr:
        biz_desc = rec.get("business_types_description", "").upper()
        is_nonprofit = any(x in biz_desc for x in ["NONPROFIT", "501C3"])
        if is_nonprofit and rcity in bmf_index.get("by_city", {}):
            for c in bmf_index["by_city"][rcity]:
                addr_sim = _address_similarity(raddr, c.get("STREET", ""))
                if addr_sim >= 0.9:
                    return c, 0.75, f"addr_only_nonprofit (addr={addr_sim:.2f})"

    # Pass 3: Token globally — DISABLED for first pass (O(N*M) too slow at scale)
    # Re-enable with an inverted index (tokens→records) for production
    # if best_score < 0.7:
    #     ... scans all 1.95M BMF records per unmatched recipient

    if best_match and best_score >= 0.55:
        return best_match, best_score, best_method
    return None, 0, None
