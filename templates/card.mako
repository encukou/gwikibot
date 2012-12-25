
__NOTOC__[[File:${'TODO'}|200px|right]]

'''Articuno''' (Japanese: '''フリーザー''' ''Freezer'') is a Water-type Basic Pokemon card originally from Fossil.

It has also been reprinted in Legendary Collection, in Expedition, and as a promo card / as X promo cards.

==Card Text==
'''${card.name}'''
% if card.types:
- ${'/'.join(t.name for t in card.types)}
% endif
% if card.hp:
- HP${card.hp}
% endif
<br>
% if card.stage:
${card.stage.name}
% endif
${card.class_.name}

% for mechanic in card.mechanics:
 % for cost in mechanic.costs:
${'[{}]'.format(cost.type.initial) * cost.amount}
 % endfor
 % if not (mechanic.costs and mechanic.class_.identifier == 'attack'):
${mechanic.class_.name}:
 % endif
 % if mechanic.damage_base or mechanic.damage_modifier:
${mechanic.damage_base}${mechanic.damage_modifier} damage.
 % endif
${mechanic.effect}
 % if not loop.last:
<br>
 % endif
% endfor

% for mod in card.damage_modifiers:
 % if mod.operation in u'+×':
Weakness:
 % elif mod.operation in '-':
Resistance
 % endif
${mod.type.name} (${mod.operation}${mod.amount})
<br>
% endfor
% if retreat_cost is not None:
Retreat: ${card.retreat_cost}
% endif

==English Card Scans==
<gallery>
File:Articuno Fossil 2.jpg|Fossil #2
File:Articuno Fossil 2.jpg|Fossil #17
File:Articuno Fossil 2.jpg|Legendary Collection #2
</gallery>

==Release Info==
Articuno was released as
