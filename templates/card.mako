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
__NOTOC__{{CardInfoBox
    |cardname=${card.name}
    % for i, t in enumerate(card.types):
    |type${i + 1}=${t.name}
    % endfor
    % for s in last_print.scans:
    |filename=${s.filename}
    <% break %>
    % endfor
    |set=${last_print.set.name}
    |cardno=#${last_print.set_number}${'/{}'.format(last_print.set.total) if last_print.set.total else ''}
    |rarity=${last_print.rarity.name[0]}${'H' if last_print.holographic else ''}
    |illus=${last_print.illustrator.name}
    % if last_print.pokemon_flavor:
        % if last_print.pokemon_flavor.species:
        |pkmnclass=The ${last_print.pokemon_flavor.genus} Pokémon
        % endif
        % if last_print.pokemon_flavor.height:
        |length=Length: ${u'{}′{}″'.format(*divmod(last_print.pokemon_flavor.height, 12))}"
        % endif
        % if last_print.pokemon_flavor.weight:
        |weight=Weight: ${last_print.pokemon_flavor.weight} lbs.
        % endif
        % if last_print.pokemon_flavor.dex_entry:
        |pokedex=${last_print.pokemon_flavor.dex_entry}
        % endif
    % endif
}}

'''${card.name}''' is a
% if card.types:
${'/'.join('[[TCG:{0}|{0}]]'.format(t.name) for t in card.types)}-type
% endif
${u'[[TCG:{0}|{0}]]'.format(
    (card.stage.name + ' ' if card.stage else '') +
    card.class_.name)}
card originally from ${set_link(first_print.set)}.

% if any(s is not first_print.set for s in card_sets):
It has also been reprinted in ${and_join((set_link(s) for s in card_sets if s is not first_print.set))}.
% endif

==Card Text==
'''${card.name}'''
% if card.types:
– ${'/'.join(t.name for t in card.types)}
% endif
% if card.hp:
– HP${card.hp}
% endif
<br>
% if card.stage:
${card.stage.name}
% endif
${card.class_.name}

% for mechanic in card.mechanics:
% if mechanic.costs:
${''.join('{}'.format(cost.type.initial) * cost.amount for cost in mechanic.costs)}
% endif
 % if not (mechanic.costs and mechanic.class_.identifier == 'attack'):
${mechanic.class_.name}:
 % endif
 % if mechanic.name:
'''${mechanic.name}''' –
 % endif
 % if mechanic.damage_base or mechanic.damage_modifier:
${mechanic.damage_base}${mechanic.damage_modifier if mechanic.damage_modifier else ''} damage.
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
Resistance:
 % endif
${mod.type.name} (${mod.operation}${mod.amount})
<br>
% endfor
% if card.retreat_cost is not None:
Retreat: ${card.retreat_cost}
% endif

==English Releases==
<gallery>
% for p in prints:
File:${p.scans[0].filename}.jpg|[[TCG:${p.set.name}|${p.set.name}]]<br />${p.set_number}${'/{}'.format(p.set.total) if p.set.total else ''}, ${p.rarity.symbol}${'H' if p.holographic else ''}
% endfor
</gallery>

{{TCG:${card.name} ${first_print.set.name} ${first_print.set_number} Release Info}}
