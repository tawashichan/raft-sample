use std::collections::HashMap;

struct History {
    states: Vec<State>,
    events: Vec<Event>,
}

#[derive(Clone, Debug)]
struct State {
    time: i64,
    nodes: HashMap<NodeId, Node>,
    requests: Vec<Request>,
    replies: Vec<Reply>,
}

#[derive(Clone, Debug, PartialEq)]
enum NodeState {
    Leader,
    Follower,
    Candidate,
}
#[derive(Clone, Debug, PartialEq)]
enum NodeActivity {
    Active,
    Stopped,
}

#[derive(Clone, Debug, PartialOrd, PartialEq)]
struct Term(i64);
#[derive(Clone, Debug)]
struct CommitIndex(i64);
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct NodeId(i64);
#[derive(Clone, Debug)]
struct Log {
    term: i64,
    value: i64,
}

#[derive(Clone, Debug)]
struct Node {
    id: NodeId,
    activity: NodeActivity,
    logs: Vec<Log>,
    state: NodeState,
    current_term: Term,
    voted_for: Option<NodeId>,
    commit_index: CommitIndex,
    next_append_entry_due: Option<i64>,
    default_append_entry_due: i64,
    next_election_due: Option<i64>, //何ターン後にrequestvoteするか
    default_next_election_due: i64,
    default_request_time_to_take: i64,
    peers: Peers,
}

type Peers = HashMap<NodeId, Peer>;

#[derive(Clone, Debug)]
struct Peer {
    next_index: i64,
    match_index: i64,
    vote_granted: bool,
}

#[derive(Clone, Debug)]
struct AppendEntriesResp {
    term: Term,
    from: NodeId,
    to: NodeId,
    success: bool,
    match_index: i64,
    time_to_take: i64, //到達までの所要時間
}

#[derive(Clone, Debug)]
struct RequestVoteResp {
    from: NodeId,
    to: NodeId,
    term: Term,
    granted: bool,
    time_to_take: i64, //到達までの所要時間
}

#[derive(Clone, Debug)]
enum Reply {
    AppendEntries(AppendEntriesResp),
    RequestVote(RequestVoteResp),
}

#[derive(Clone, Debug)]
struct RequestVoteReq {
    term: Term,
    from: NodeId,
    to: NodeId,
    last_log_index: i64,
    last_log_term: i64,
    time_to_take: i64, //到達までの所要時間
}

#[derive(Clone, Debug)]
struct AppendEntriesReq {
    term: Term,
    from: NodeId,
    to: NodeId,
    prev_index: i64, // 何のためにある?
    prev_term: i64,  // 何のためにある?
    entries: Vec<Log>,
    commit_index: i64,
    time_to_take: i64, //到達までの所要時間
}

#[derive(Clone, Debug)]
enum Request {
    RequestVote(RequestVoteReq),
    AppendEntries(AppendEntriesReq),
}

enum Event {
    Stop(NodeId, i64),
    Resume(NodeId, i64),
    Request(i64, i64),
}

impl Event {
    fn time(&self) -> &i64 {
        match self {
            Event::Stop(_, time) => time,
            Event::Resume(_, time) => time,
            Event::Request(_, time) => time,
        }
    }
}

impl Node {
    pub fn new(
        id: NodeId,
        default_append_entry_due: i64,
        default_next_election_due: i64,
        default_request_time_to_take: i64,
    ) -> Self {
        Node {
            id: id,
            activity: NodeActivity::Active,
            logs: vec![],
            state: NodeState::Follower,
            current_term: Term(0),
            voted_for: None,
            commit_index: CommitIndex(0),
            next_append_entry_due: None,
            default_append_entry_due: default_append_entry_due,
            next_election_due: Some(default_next_election_due),
            default_next_election_due: default_next_election_due,
            default_request_time_to_take: default_request_time_to_take,
            peers: HashMap::new(),
        }
    }

    fn last_log_index(&self) -> i64 {
        let len = self.logs.len() as i64;
        if len == 0 {
            0
        } else {
            len - 1
        }
    }

    fn last_log_term(&self) -> i64 {
        if self.logs.len() == 0 {
            0
        } else {
            let last_log = self.logs.last().unwrap();
            last_log.term
        }
    }

    fn stopped(&self) -> bool {
        self.activity == NodeActivity::Stopped
    }

    fn request_vote(&mut self, node_ids: &[NodeId]) -> Vec<Request> {
        if self.stopped() {
            return vec![];
        }

        self.current_term = Term(self.current_term.0 + 1);
        self.voted_for = Some(self.id.clone());
        self.next_election_due = Some(self.default_next_election_due);
        node_ids
            .iter()
            .filter_map(|id| {
                if id == &self.id {
                    None
                } else {
                    Some(Request::RequestVote(RequestVoteReq {
                        from: self.id.clone(),
                        to: id.clone(),
                        last_log_index: self.last_log_index(),
                        time_to_take: self.default_request_time_to_take,
                        last_log_term: self.last_log_term(),
                        term: self.current_term.clone(),
                    }))
                }
            })
            .collect()
    }

    // リーダーとフォロワーでログの不整合が発生した場合,不整合が発生しなくなるところまでリーダーは配信するログを巻き戻す(nextIndexをdecrementする)
    fn append_entry(&self, node_ids: &[NodeId]) -> Vec<Request> {
        if self.stopped() {
            return vec![];
        }

        node_ids
            .iter()
            .filter_map(|id| {
                if id == &self.id {
                    None
                } else {
                    Some(Request::AppendEntries(AppendEntriesReq {
                        term: self.current_term.clone(),
                        from: self.id.clone(),
                        to: id.clone(),
                        prev_index: 0, //　ちゃんと埋める
                        prev_term: 0,  //ちゃんと埋める
                        commit_index: 0,
                        entries: vec![], //ちゃんと埋める
                        time_to_take: self.default_request_time_to_take,
                    }))
                }
            })
            .collect()
    }

    fn apply_request(&mut self, req: Request) -> Option<Reply> {
        if self.stopped() {
            return None;
        }
        match req {
            Request::RequestVote(body) => {
                if body.term > self.current_term {
                    self.current_term = body.term.clone();
                    self.voted_for = None
                }
                let last_log_term = self.last_log_term();
                let last_log_index = self.last_log_index();
                // 誰にも投票していないか自分に投票していて,なおかつ保持している(commitされている,ではない!)ログが自分の最新のものよりtermが同等か新しいかつindexが同等か新しければok
                let granted = body.term >= self.current_term
                    && (self.voted_for == None || self.voted_for == Some(body.from.clone()))
                    && body.last_log_index >= last_log_index
                    && body.last_log_term >= last_log_term;
                if granted {
                    self.voted_for = Some(body.from.clone());
                    self.state = NodeState::Follower;
                    self.peers = HashMap::new();
                }
                Some(Reply::RequestVote(RequestVoteResp {
                    from: self.id.clone(),
                    to: body.from,
                    time_to_take: self.default_request_time_to_take,
                    term: self.current_term.clone(),
                    granted: granted,
                }))
            }
            Request::AppendEntries(body) => {
                let success = body.term >= self.current_term;
                let match_index = 0;
                self.next_election_due = Some(self.default_next_election_due);
                Some(Reply::AppendEntries(AppendEntriesResp {
                    term: self.current_term.clone(),
                    from: self.id.clone(),
                    to: body.from,
                    success: success,
                    match_index: match_index,
                    time_to_take: self.default_request_time_to_take,
                }))
            }
        }
    }

    fn apply_reply(&mut self, node_ids: &[NodeId], rep: Reply) {
        if self.stopped() {
            return;
        }

        match rep {
            Reply::RequestVote(body) => {
                if body.granted {
                    match self.peers.get_mut(&body.from) {
                        Some(peer) => {
                            peer.vote_granted = true;
                        }
                        None => {
                            self.peers.insert(
                                body.from.clone(),
                                Peer {
                                    vote_granted: true,
                                    next_index: 0,
                                    match_index: 0,
                                },
                            );
                        }
                    }
                    let voted_count =
                        self.peers.iter().fold(
                            0,
                            |sum, (_, peer)| if peer.vote_granted { sum + 1 } else { sum },
                        );
                    // 過半数
                    if voted_count > node_ids.len() / 2 && self.state == NodeState::Candidate {
                        self.state = NodeState::Leader;
                        self.next_append_entry_due = Some(0);
                        self.next_election_due = None;
                    }
                } else {
                    if body.term > self.current_term {
                        self.current_term = body.term
                    }
                }
            }
            Reply::AppendEntries(body) => {
                //実装しようね
            }
        }
    }
}

impl History {
    fn new(events: Vec<Event>) -> History {
        History {
            states: vec![],
            events: events,
        }
    }

    fn get_history(&mut self, initial_state: State, target_time: i64) {
        let mut state = initial_state;
        for _ in 0..target_time {
            state = state.next_state(&self.events);
            self.states.push(state.clone())
        }
    }
}

impl State {
    fn new(nodes: Vec<Node>) -> Self {
        let mut node_map = HashMap::new();
        for node in nodes.iter() {
            node_map.insert(node.id.clone(), node.clone());
        }

        State {
            time: 0,
            nodes: node_map,
            requests: vec![],
            replies: vec![],
        }
    }

    fn next_state(self, events: &[Event]) -> Self {
        let time = self.time + 1;
        let state = State { time: time, ..self };
        let events: Vec<&Event> = events.iter().filter(|ev| ev.time() == &time).collect();
        let State {
            time,
            mut nodes,
            requests,
            replies,
        } = events.iter().fold(state, |sum, ev| sum.apply_event(ev));
        //非効率だが読みやすさ重視
        let requests = requests.iter().map(|r| r.next_state());
        let replies = replies.iter().map(|r| r.next_state());

        let (reached_requests, unreached_requests): (Vec<Request>, Vec<Request>) =
            requests.partition(|r| r.reached());
        let (reached_replies, unreached_replies): (Vec<Reply>, Vec<Reply>) =
            replies.partition(|r| r.reached());

        let node_ids: Vec<NodeId> = nodes.iter().map(|(id, node)| id.clone()).collect();

        let mut new_replies = vec![];
        for req in reached_requests {
            let node = nodes.get_mut(&req.to()).unwrap();
            if let Some(reply) = node.apply_request(req) {
                new_replies.push(reply);
            }
        }

        for rep in reached_replies {
            let node = nodes.get_mut(&rep.to()).unwrap();
            node.apply_reply(&node_ids, rep);
        }
        let mut new_requests = vec![];
        nodes.iter_mut().for_each(|(_, node)| {
            match node.state {
                NodeState::Leader => match node.next_append_entry_due {
                    Some(due) if due > 0 => node.next_append_entry_due = Some(due - 1),
                    Some(_) => {
                        let mut requests = node.append_entry(&node_ids);
                        new_requests.append(&mut requests);
                    }
                    None => node.next_append_entry_due = Some(node.default_append_entry_due),
                },
                NodeState::Follower => match node.next_election_due {
                    None => node.next_election_due = Some(node.default_next_election_due),
                    Some(due) if due > 0 => node.next_election_due = Some(due - 1),
                    Some(_) => {
                        //立候補
                        node.state = NodeState::Candidate;
                        let mut requests = node.request_vote(&node_ids);
                        new_requests.append(&mut requests)
                    }
                },
                NodeState::Candidate => {
                    match node.next_election_due {
                        None => node.next_election_due = Some(node.default_next_election_due),
                        Some(due) if due > 0 => node.next_election_due = Some(due - 1),
                        Some(_) => {
                            // timeoutしたらまた立候補
                            node.state = NodeState::Candidate;
                            let mut requests = node.request_vote(&node_ids);
                            new_requests.append(&mut requests)
                        }
                    }
                }
            }
        });

        let mut requests = unreached_requests;
        requests.append(&mut new_requests);
        let mut replies = unreached_replies;
        replies.append(&mut new_replies);

        State {
            time: time,
            nodes: nodes,
            requests: requests,
            replies: replies,
        }
    }

    fn apply_event(self, event: &Event) -> Self {
        match event {
            Event::Stop(id, _) => {
                let mut nodes = self.nodes;
                let mut node = nodes.get_mut(id).unwrap();
                node.activity = NodeActivity::Stopped;
                State { nodes, ..self }
            }
            Event::Resume(id, _) => {
                let mut nodes = self.nodes;
                let mut node = nodes.get_mut(id).unwrap();
                if node.stopped() {
                    node.activity = NodeActivity::Active;
                    node.state = NodeState::Follower
                }
                State { nodes, ..self }
            }
            Event::Request(val, _) => self,
        }
    }
}

impl Request {
    fn next_state(&self) -> Self {
        match self {
            Request::AppendEntries(req) => Request::AppendEntries(AppendEntriesReq {
                time_to_take: req.time_to_take - 1,
                ..req.clone()
            }),
            Request::RequestVote(req) => Request::RequestVote(RequestVoteReq {
                time_to_take: req.time_to_take - 1,
                ..req.clone()
            }),
        }
    }

    fn reached(&self) -> bool {
        match self {
            Request::AppendEntries(req) => req.time_to_take == 0,
            Request::RequestVote(req) => req.time_to_take == 0,
        }
    }

    fn to(&self) -> NodeId {
        match self {
            Request::AppendEntries(req) => req.to.clone(),
            Request::RequestVote(req) => req.to.clone(),
        }
    }
}

impl Reply {
    fn next_state(&self) -> Self {
        match self {
            Reply::AppendEntries(req) => Reply::AppendEntries(AppendEntriesResp {
                time_to_take: req.time_to_take - 1,
                ..req.clone()
            }),
            Reply::RequestVote(req) => Reply::RequestVote(RequestVoteResp {
                time_to_take: req.time_to_take - 1,
                ..req.clone()
            }),
        }
    }

    fn reached(&self) -> bool {
        match self {
            Reply::AppendEntries(req) => req.time_to_take == 0,
            Reply::RequestVote(req) => req.time_to_take == 0,
        }
    }

    fn to(&self) -> NodeId {
        match self {
            Reply::AppendEntries(req) => req.to.clone(),
            Reply::RequestVote(req) => req.to.clone(),
        }
    }
}

fn main() {
    let nodes = vec![
        Node::new(NodeId(1), 1, 2, 1),
        Node::new(NodeId(2), 1, 3, 1),
        Node::new(NodeId(3), 1, 10, 1),
        Node::new(NodeId(4), 1, 7, 1),
        Node::new(NodeId(5), 1, 9, 1),
    ];
    let initial_state = State::new(nodes);
    let events = vec![]; //vec![Event::Stop(NodeId(1), 6)];
    let mut history = History::new(events);
    history.get_history(initial_state, 10);
    for state in history.states {
        println!(
            "time: {:?}\nnodes1: {:?}\nnode2: {:?}\nnode3: {:?}\nnode4: {:?}\nnode5: {:?}\nrequests: {:?}\nreplues: {:?}\n",
            state.time,
            state.nodes[&NodeId(1)],
            state.nodes[&NodeId(2)],
            state.nodes[&NodeId(3)],
            state.nodes[&NodeId(4)],
            state.nodes[&NodeId(5)],
            state.requests,
            state.replies,
        );
    }
}

#[test]
fn leader_election() {
    let nodes = vec![];
    let initial_state = State::new(nodes);
    let mut history = History::new();
    history.get_history(initial_state, 100);
    println!("{:?}", history.states);
}
