@0x934efea7f017fff0;

struct Entry {
  id @0 :Text;
  label @1 :Text;
  aliases @2 :List(Text);
  description @3 :Text;
}

struct Chunk {
  entries @0 :List(Entry);
}
