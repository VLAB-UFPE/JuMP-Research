from pm4py.objects.petri_net.obj import PetriNet, Marking

def get_place_name(place: PetriNet.Place):
    act_index = place.name.index("_") + 1
    return place.name[act_index:]

def convert_hid_to_dfg(transition: PetriNet.Transition, im: Marking, fm: Marking,
                      dfg: dict, ea: dict, sa: dict) -> tuple[dict, dict, dict]:
    sa_weight = 0
    ea_weight = 0
    out_transitions = dict()
    in_transitions = dict()
    for i in transition.in_arcs:
        if i.source in im.keys():
            sa_weight = i.weight
            continue
        in_name = get_place_name(i.source)
        in_transitions[in_name] = i.weight
    for i in transition.out_arcs:
        if i.target in fm.keys():
            ea_weight = i.weight
            continue
        out_name = get_place_name(i.target)
        out_transitions[out_name] = i.weight
    
    for in_act, weight in in_transitions.items():
        for out_act in out_transitions:
            dfg[(in_act, out_act)] = weight
    
    if sa_weight > 0:
        for act in out_transitions:
            sa[act] = sa_weight
    if ea_weight > 0:
        for act in in_transitions:
            ea[act] = ea_weight
    return dfg, sa, ea

def convert_tr_to_dfg(tr: PetriNet.Transition, im: Marking, fm: Marking,
                      dfg: dict, ea: dict, sa: dict) -> tuple[dict, dict, dict]:
    for i in tr.in_arcs:
        if i.source in im.keys():
            sa[tr.name] = i.weight
            continue
        name = get_place_name(i.source)
        if name != tr.name:
            dfg[(name, tr.name)] = i.weight
    for i in tr.out_arcs:
        if i.target in fm.keys():
            ea[tr.name] = i.weight
            continue
        name = get_place_name(i.target)
        if name != tr.name:
            dfg[(tr.name, name)] = i.weight
    return dfg, sa, ea

def convert_pn_to_dfg(pn: PetriNet, im: Marking, fm: Marking
                      ) -> tuple[dict, dict, dict]:
    dfg, start_activities, end_activities = dict(), dict(), dict()
    for i in pn.transitions:
        if "hid_" in i.name:
            dfg, start_activities, end_activities = convert_hid_to_dfg(
                i, im, fm, dfg, end_activities, start_activities)
        else:
            dfg, start_activities, end_activities = convert_tr_to_dfg(
                i, im, fm, dfg, end_activities, start_activities)
    return dfg, start_activities, end_activities
