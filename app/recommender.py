"""Full-antecedent matching, distinct products, explicit popularity fallback."""
import pandas as pd

COLUMNS = ['product_id', 'product_name', 'score', 'source', 'source_rule',
           'rule_id', 'support', 'confidence', 'lift', 'purchase_count']


def recommend(basket, lookup, popular, catalog, k=5):
    selected = {int(x) for x in basket}
    names = dict(zip(catalog['product_id'].astype(int), catalog['product_name']))
    candidates = []
    if not lookup.empty:
        seeds = lookup[lookup['antecedent_product_id'].isin(selected)].drop_duplicates('rule_id')
        for rule in seeds.to_dict('records'):
            antecedent = {int(x) for x in rule['antecedent']}
            product = int(rule['recommended_product_id'])
            if not antecedent or not antecedent.issubset(selected) or product in selected:
                continue
            if float(rule['lift']) <= 1 or len(rule['consequent']) != 1:
                continue
            candidates.append({'product_id': product, 'product_name': names.get(product, str(product)),
                'score': float(rule['confidence']), 'source': 'Luật kết hợp',
                'source_rule': ', '.join(rule['antecedent_names'])+' → '+', '.join(rule['consequent_names']),
                'rule_id': rule['rule_id'], 'support': float(rule['support']),
                'confidence': float(rule['confidence']), 'lift': float(rule['lift']), 'purchase_count': None})
    if candidates:
        result = pd.DataFrame(candidates).sort_values(
            ['confidence', 'lift', 'support', 'product_id'], ascending=[False, False, False, True])
        return result.drop_duplicates('product_id').head(k).reindex(columns=COLUMNS)
    # Fallback only when no matching rule. Counts remain real, no invented confidence/lift.
    fallback = []
    for row in popular.sort_values(['count', 'product_id'], ascending=[False, True]).to_dict('records'):
        product = int(row['product_id'])
        if product in selected:
            continue
        fallback.append({'product_id': product, 'product_name': row['product_name'], 'score': None,
            'source': 'Fallback phổ biến', 'source_rule': 'Không có luật khớp; xếp theo lượt mua quan sát',
            'rule_id': None, 'support': None, 'confidence': None, 'lift': None,
            'purchase_count': int(row['count'])})
    return pd.DataFrame(fallback, columns=COLUMNS).drop_duplicates('product_id').head(k)
