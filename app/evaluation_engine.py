"""Target-free deterministic recommendation engine shared by evaluation and UI."""
import math

METHODS = ('popularity', 'rule_only', 'hybrid')


class RuleEngine:
    def __init__(self, rules, popularity):
        self.rules, self.index = {}, {}
        for value in rules:
            rule = dict(value)
            antecedent = tuple(sorted(int(x) for x in rule['antecedent']))
            consequent = tuple(int(x) for x in rule['consequent'])
            if (not antecedent or len(set(antecedent)) != len(antecedent) or len(consequent) != 1 or
                    min(antecedent + consequent) <= 0 or set(antecedent) & set(consequent)):
                raise ValueError('Invalid rule items')
            if any(not math.isfinite(float(rule[key])) for key in ('confidence', 'support', 'lift')):
                raise ValueError('Nonfinite rule metric')
            if not (0 <= rule['confidence'] <= 1 and 0 <= rule['support'] <= 1 and rule['lift'] > 1):
                raise ValueError('Invalid rule metric')
            rule.update(antecedent=antecedent, consequent=consequent)
            old = self.rules.get(rule['rule_id'])
            if old is not None:
                if old != rule:
                    raise ValueError('Conflicting duplicate rule_id')
                continue
            self.rules[rule['rule_id']] = rule
            for seed in antecedent:
                self.index.setdefault(seed, []).append(rule['rule_id'])
        self.popularity = sorted([(int(r['product_id']), int(r['basket_count'])) for r in popularity],
                                 key=lambda r: (-r[1], r[0]))
        if len({r[0] for r in self.popularity}) != len(self.popularity) or any(min(r) <= 0 for r in self.popularity):
            raise ValueError('Invalid training popularity')

    def predict(self, context, method='hybrid', k=5):
        if method not in METHODS or k <= 0:
            raise ValueError('Unknown method or invalid K')
        selected = {int(x) for x in context}
        output = []
        if method != 'popularity':
            candidates = set(rule for seed in selected for rule in self.index.get(seed, []))
            eligible = [self.rules[key] for key in candidates if set(self.rules[key]['antecedent']) <= selected
                        and self.rules[key]['consequent'][0] not in selected]
            eligible.sort(key=lambda r: (-r['confidence'], -r['lift'], -r['support'], r['consequent'][0], r['rule_id']))
            seen = set(selected)
            for rule in eligible:
                product = rule['consequent'][0]
                if product in seen:
                    continue
                seen.add(product)
                output.append({'product_id': product, 'source': 'rule', 'rule_id': rule['rule_id'],
                               'confidence': float(rule['confidence']), 'lift': float(rule['lift']),
                               'support': float(rule['support']), 'basket_count': None})
                if len(output) == k:
                    break
        if method != 'rule_only' and len(output) < k:
            seen = selected | {row['product_id'] for row in output}
            for product, count in self.popularity:
                if product not in seen:
                    seen.add(product)
                    output.append({'product_id': product, 'source': 'popularity', 'rule_id': None,
                                   'confidence': None, 'lift': None, 'support': None, 'basket_count': count})
                    if len(output) == k:
                        break
        return output


def score_case(predictions, targets, k=5):
    truth = set(targets)
    if not truth:
        raise ValueError('Empty target')
    products = [row['product_id'] for row in predictions]
    if len(products) != len(set(products)) or len(products) > k:
        raise ValueError('Invalid distinct top-K predictions')
    hits = len(set(products) & truth)
    return {'hit_rate_at_5': float(hits > 0), 'precision_at_5': hits / k,
            'recall_at_5': hits / len(truth), 'recommendation_coverage': float(bool(products)),
            'rule_coverage': float(any(row['source'] == 'rule' for row in predictions))}
