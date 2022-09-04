import { LCDClient } from "@terra-money/terra.js";
import fs from "fs";

let ts = "https://mantlemint-1.terra.p2p.org";
let ts2 = "http://mantle-archival-private-www-673419417.ap-northeast-2.elb.amazonaws.com/";
let ts3 = "https://columbus-lcd.terra.dev";


const terra = new LCDClient({
  URL: ts2,
  chainID: "columbus-5",
});

let SNAPSHOT_1 = 7544910;
let SNAPSHOT_2 = 7790000;

let BLOCK = SNAPSHOT_1;
// let astro_ust_astroport_pool_address =
//   "terra1l7xu2rl3c7qmtx3r5sd2tz25glf6jh8ul7aag7";
// let astro_ust_terraswap_pool_address =
//   "terra1pufczag48fwqhsmekfullmyu02f93flvfc9a25";
// let astro_uluna_pool_address = "terra1nujm9zqa4hpaz9s8wrhrp86h3m9xwprjt9kmf9";
// let astro_staking_contract = "terra1f68wt2ch3cx2g62dxtc8v68mkdh5wchdgdjwz7";

let xastro_contract = "terra14lpnyzc9z4g3ugr4lhm8s4nle0tq8vcltkhzh7";

// let reactor_contract = "terra1jnf3m3rkns52husav43zyzc857wxts00vdr8j2";

while (true) {
  let block = BLOCK;
  console.log(`block #${block}`);

  let xastro_balance = await terra.wasm.contractQuery(
    xastro_contract,
    { balance: { address: "terra16gxe9q6sjx7sz9d8hqfyy27e5m5satjdgrlukd" } },
    // {
    //   height: ${BLOCK},
    // }
  );
  console.log(xastro_balance);
  // break
}