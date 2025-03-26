// fn foo() {
//     let mut positions = (0..(K - 1))
//         .map(|_| rng.next_u64() % self.position_map.len() as u64)
//         .collect::<Vec<_>>();
//
//     // 将index随机插入到addresses中
//     let len = positions.len();
//     positions.push(index);
//     let i = rng.next_u64() as usize % len;
//     positions.swap(i, len);
//
//     let mapping_positions = positions
//         .iter()
//         .map(|&i| self.position_map[i as usize])
//         .collect::<Vec<_>>();
//
//     // read: MGET(mapping_positions)
//     let mut path = mapping_positions
//         .iter()
//         .map(|&i| self.physical_memory[i as usize].blocks[0].value)
//         .collect::<Vec<_>>();
//
//     // send path 记录流量
//     let bytes = path
//         .iter()
//         .map(|x| x.data.as_slice())
//         .collect::<Vec<_>>()
//         .concat();
//     CLIENT.with(|client| {
//         client.borrow_mut().write_all(&bytes).unwrap();
//     });
//
//     let result = callback(&path[i]);
//
//     // shuffle
//     shuffle_vec(&mut positions, &mut path, rng);
//
//     // write: MSET(mapping_positions, stash)
//     for (i, &pos) in mapping_positions.iter().enumerate() {
//         self.physical_memory[pos as usize].blocks[0].value = path[i];
//     }
//
//     // send mapping_positions and path 记录流量
//     let mut bytes = path
//         .iter()
//         .map(|x| x.data.as_slice())
//         .collect::<Vec<_>>()
//         .concat();
//     bytes.extend_from_slice(
//         &positions
//             .iter()
//             .map(|p| p.to_ne_bytes())
//             .collect::<Vec<_>>()
//             .concat(),
//     );
//     CLIENT.with(|client| {
//         client.borrow_mut().write_all(&bytes).unwrap();
//     });
//
//     // sync position_map
//     for (i, &pos) in positions.iter().enumerate() {
//         self.position_map[pos as usize] = mapping_positions[i];
//     }
// }
