<%
card_sets = sorted(set(p.set for p in prints), key=lambda s: s.id)

def set_link(set):
    return '[[{}|{}]]'.format(wm.set_page_title(set), set.name)

def and_join(words):
    words = list(words)
    if words:
        return ', and '.join([
            ', '.join(words[:-1]),
            words[-1]
        ])
    else:
        return ''

%>

{{DISPLAYTITLE:${card.name} (${first_print.set.name} #${first_print.set_number})}}
__NOTOC__{{CardInfoBox\
|cardname=${card.name}\
% for i, t in enumerate(card.types):
|type${i + 1}=${t.name}\
% endfor
% for s in last_print.scans:
|filename=${s.filename}\
<% break %>
% endfor
|set=${last_print.set.name}\
|cardno=#${last_print.set_number}${'/{}'.format(last_print.set.total) if last_print.set.total else ''}\
|rarity=${last_print.rarity.name[0]}${'H' if last_print.holographic else ''}\
|illus=${', '.join(il.name for il in last_print.illustrators)}\
% if last_print.pokemon_flavor:
% if last_print.pokemon_flavor.species:
|pkmnclass=The ${last_print.pokemon_flavor.genus} Pokémon\
% endif
% if last_print.pokemon_flavor.height:
|length=Length: ${u'{}′{}″'.format(*divmod(last_print.pokemon_flavor.height, 12))}"\
% endif
% if last_print.pokemon_flavor.weight:
|weight=Weight: ${last_print.pokemon_flavor.weight} lbs.\
% endif
% if last_print.pokemon_flavor.dex_entry:
|pokedex=${last_print.pokemon_flavor.dex_entry}\
% endif
% endif
}}

'''${card.name}''' is a \
% if card.types:
${'/'.join('[[TCG:{0}|{0}]]'.format(t.name) for t in card.types)}-type \
% endif
${u'[[TCG:{0}|{0}]]'.format(
    (card.stage.name + ' ' if card.stage else '') +
    card.class_.name)}
card originally from ${set_link(first_print.set)}. \

% if any(s is not first_print.set for s in card_sets):
It has also been reprinted in ${and_join((set_link(s) for s in card_sets if s is not first_print.set))}.\
% endif

==Card Text==
{{Card Spoiler|\
class=${card.class_.name}|\
% if card.class_.identifier=='pokemon':
% for i, t in enumerate(card.types):
parameter${i + 1}=${t.name}|\
%endfor
% elif card.class_.identifier=='energy':
% for i, t in enumerate(card.subclasses):
parameter${i + 1}=${t.name}|\
%endfor
%endif
card=${card.name}|\
% for i, t in enumerate(card.types):
type${i + 1}=${t.name}|\
%endfor
% if card.hp:
hp=${card.hp}|\
% endif
% if card.stage:
stage=${card.stage.name} Pokémon|\
% endif
<% attack_numbers = dict() %>\
% for mechanic in card.mechanics:
<%
    slug = dict(attack='atk').get(mechanic.class_.identifier, mechanic.class_.identifier)
    mechnum = attack_numbers[slug] = attack_numbers.get(slug, 0) + 1
%>\
% if slug == 'atk':
${slug}${mechnum}cost=${''.join('{}'.format(cost.type.initial) * cost.amount for cost in mechanic.costs)}|\
${slug}${mechnum}dmg=${mechanic.damage_base}${mechanic.damage_modifier if mechanic.damage_modifier else ''}|\
% endif
% if mechanic.name:
${slug}${mechnum}name=${mechanic.name}|\
% endif
${slug}${mechnum}=${mechanic.effect}|\
% endfor
% for mod in card.damage_modifiers:
% if mod.operation in u'+×':
weakness=${mod.type.name}|\
weaknessamt=${mod.operation}${mod.amount}|\
% else:
resistance=${mod.type.name}|\
resistanceamt=${mod.operation}${mod.amount}|\
% endif
% endfor
% if card.retreat_cost is not None:
retreat=${card.retreat_cost}|\
% endif
}}

==English Releases==
<gallery>
% for p in prints:
File:${p.scans[0].filename}.jpg|[[TCG:${p.set.name}|${p.set.name}]]<br />${p.set_number}${'/{}'.format(p.set.total) if p.set.total else ''}, ${p.rarity.symbol}${'H' if p.holographic else ''}
% endfor
</gallery>

{{TCG:${card.name} ${first_print.set.name} ${first_print.set_number} Release Info}}
