This is the wiki page for ${tcg_set.name}

== Card List ==
% for print_ in tcg_set.prints:
${print_.set_number}. [[${wm.card_page_title(print_.card)} | ${print_.card.name}]]
 % if not loop.last:
<br>
 % endif
% endfor
