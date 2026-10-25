// Player-safe data for the World Codex player page.
// This file is loaded by the PUBLIC player page. It must ONLY ever contain
// player-visible content — nothing private is stored here.
window.PLAYER_DATA = {
  categories: [
    { id: 2, name: "Cosmology", description: "How the world began — the twins, the United being, and the fracture.", sortOrder: 1 },
    { id: 3, name: "The Twins & The Shards", description: "Vaelos, Khavos, and the pantheon their fracture created.", sortOrder: 2 },
    { id: 1, name: "Realms", description: "Aethelis and Khavora — the first realms, and the only two afterlives.", sortOrder: 3 }
  ],
  articles: [
    {
      id: 1, title: "The Creation: The Twins and the Fracture", type: "Myth", categoryId: 2,
      summary: "In the beginning there was nothing but twin brothers — Order and Chaos. Their cooperation created the universe, the world, and life — and broke them apart.",
      body: `In the beginning there was nothing except twin brothers: Order and Chaos. Neither could create life alone.

Order — [[Vaelos]] — made a timeless, static, beautiful and idyllic realm, with nothing alive in it. Chaos — [[Khavos]] — made a primal realm where all time exists at once, where void and light fight, and where opposites, too hot and too cold, coexist.

Bored with their own realms, the brothers cooperated to create the universe, the world, and life. But cooperating merged them into one unstable United being. That being created the world and souls — and then fractured into many pieces: [[The Shards]].

What remains of the United being still sits at the core of the world, broken-off shards scattered from it, and it is the source of all magic.

From the fracture came a pantheon: patron gods of the races, and aspects of light, darkness, time, wind, sea, and fire — each with its own personality and goals, sometimes cooperating, sometimes in conflict.

The brothers' first realms never went away. [[Aethelis, the Unchanging]] and [[Khavora, the Maelstrom Womb]] still exist, and they are the only two destinations for the dead. See [[The Two Afterlives]].`,
      visibility: "player", tags: ["creation", "twins", "united being", "fracture", "cosmology"], pinned: true
    },
    {
      id: 7, title: "The Two Afterlives", type: "Myth", categoryId: 2,
      summary: "There are only two places the dead can go — Aethelis or Khavora — and which one counts as heaven depends entirely on who you ask.",
      body: `There is no long list of heavens and hells. [[Vaelos]]'s realm, [[Aethelis, the Unchanging]], and [[Khavos]]'s realm, [[Khavora, the Maelstrom Womb]], still exist — and they are the only two afterlife destinations.

Good and evil are not objective facts of the universe. They are labels the races use: what benefits a people gets called good, and what directly opposes them gets called evil. The afterlives work the same way.

Elves call Aethelis the good afterlife, because there they can become their perfect selves. Orcs call Khavora the good afterlife, because there they can fight forever. Both are telling the truth — about themselves.`,
      visibility: "player", tags: ["afterlife", "death", "good and evil", "religion"], pinned: false
    },
    {
      id: 2, title: "Vaelos", type: "Character", categoryId: 3,
      summary: "Vaelos is Order — one of the twin brothers who stood alone in the nothingness before creation, and maker of Aethelis, the Unchanging.",
      body: `Vaelos is Order, twin brother of [[Khavos]]. Before there was a world, there were only the two of them in the nothingness.

Alone, Vaelos made [[Aethelis, the Unchanging]]: a timeless, static, beautiful, idyllic realm — perfect, and completely empty of life. Order alone could not create life.

When the brothers finally cooperated to create the universe, they merged into a single unstable United being, which fractured into [[The Shards]]. See [[The Creation: The Twins and the Fracture]].

Vaelos's first realm endures. Aethelis is one of only two destinations for the dead — see [[The Two Afterlives]] — and the elves hold it to be the good one: there, they can become their perfect selves.`,
      visibility: "player", tags: ["order", "twin", "deity", "vaelos"], pinned: false
    },
    {
      id: 6, title: "Khavos", type: "Character", categoryId: 3,
      summary: "Khavos is Chaos — one of the twin brothers who stood alone in the nothingness before creation, and maker of Khavora, the Maelstrom Womb.",
      body: `Khavos is Chaos, twin brother of [[Vaelos]]. Before there was a world, there were only the two of them in the nothingness.

Alone, Khavos made [[Khavora, the Maelstrom Womb]]: a primal realm where all time exists at once, where void and light fight, and where opposites — too hot and too cold — coexist. For all its wildness, Chaos alone could not create life either.

When the brothers finally cooperated to create the universe, they merged into a single unstable United being, which fractured into [[The Shards]]. See [[The Creation: The Twins and the Fracture]].

Khavos's first realm endures. Khavora is one of only two destinations for the dead — see [[The Two Afterlives]] — and the orcs hold it to be the good one: there, they can fight forever.`,
      visibility: "player", tags: ["chaos", "twin", "deity", "khavos"], pinned: false
    },
    {
      id: 5, title: "The Shards", type: "Natural Law", categoryId: 3,
      summary: "When the United being fractured, its pieces became shards — the gods and powers of the world, ranked from Time down to local spirits.",
      body: `The fragments of the fractured United being are called shards.

The United being created the world and souls. The shards gathered that soul power and formed the races of the world in their own image. The main being still remains at the core of the world, and it is the source of magic; the shards broke off from it.

The shards have a hierarchy:

1. [[Time]] — first and most powerful, and untamable. There is no time travel unless Time deems it useful.
2. The elemental shards.
3. The patron gods of the races.
4. Local spirits.

Not every shard became a power. Shards too small to influence anything were ejected into the cosmos and formed the stars; many conjoined to make the sun.

See [[The Creation: The Twins and the Fracture]].`,
      visibility: "player", tags: ["shards", "pantheon", "magic", "hierarchy", "time"], pinned: false
    },
    {
      id: 4, title: "Aethelis, the Unchanging", type: "Location", categoryId: 1,
      summary: "The timeless, static, beautiful realm made by Vaelos before the world — and one of the only two destinations for the dead.",
      body: `Aethelis, the Unchanging, is the realm [[Vaelos]] made when Order stood alone in the nothingness: timeless, static, beautiful and idyllic — and, when he made it, empty of life.

Aethelis was never replaced by creation. It still exists, and it is one of only two places the dead can go. See [[The Two Afterlives]].

Which afterlife a culture calls the good one depends on its religion. Elves revere Aethelis as the good destination: in the Unchanging, they can become their perfect selves.`,
      visibility: "player", tags: ["realm", "order", "afterlife", "plane"], pinned: false
    },
    {
      id: 3, title: "Khavora, the Maelstrom Womb", type: "Location", categoryId: 1,
      summary: "The primal realm made by Khavos before the world, where all time exists at once — and one of the only two destinations for the dead.",
      body: `Khavora, the Maelstrom Womb, is the realm [[Khavos]] made when Chaos stood alone in the nothingness: a primal realm where all time exists at once, where void and light fight, and where opposites — too hot and too cold — coexist.

Khavora was never replaced by creation. It still exists, and it is one of only two places the dead can go. See [[The Two Afterlives]].

Which afterlife a culture calls the good one depends on its religion. Orcs revere Khavora as the good destination: in the Maelstrom Womb, they can fight forever.`,
      visibility: "player", tags: ["realm", "chaos", "afterlife", "plane"], pinned: false
    }
  ],
  timeline: [
    { id: 1, title: "The Twins Alone in the Nothingness", whenLabel: "Before time", sortKey: 1, description: "There is nothing except twin brothers: Order (Vaelos) and Chaos (Khavos). Neither can create life alone.", visibility: "player", articleId: 1 },
    { id: 2, title: "The Cooperation and the Fracture", whenLabel: "The beginning of the world", sortKey: 2, description: "Bored with their own realms, the brothers cooperate to create the universe, the world, and life. Cooperating merges them into one unstable United being, which creates the world and souls — then fractures into the shards. What remains of it still sits at the world's core, the source of magic.", visibility: "player", articleId: 1 },
    { id: 3, title: "The Shards Form the Races", whenLabel: "After the fracture", sortKey: 3, description: "The shards gather soul power and form the races of the world in their own image. Shards too small to matter are ejected into the cosmos and become the stars; many conjoined form the sun.", visibility: "player", articleId: 1 }
  ]
};
