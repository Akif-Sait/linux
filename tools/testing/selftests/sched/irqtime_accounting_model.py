#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-2.0-only
"""Discrete IRQ clock model, NOT an executable Linux scheduler model.

A CPU is either in chargeable IRQ context or task context during each unit
interval. Every interval ends at an rq-clock read. IRQ exit commits elapsed
IRQ time; local projected reads include in-progress IRQs. No clock skew,
steal time, disabled accounting, or memory-ordering effects are modeled.
"""
from itertools import product


def evaluate(contexts, readers, mode):
    committed = active = prev = clock = 0
    error = negative = 0
    steps = []
    for t, (irq, local) in enumerate(zip(contexts, readers), 1):
        # End the preceding IRQ segment when leaving IRQ context.
        if not irq:
            committed += active
            active = 0
        active += irq
        exact = 1 - irq
        if mode == 'flush' and local:
            committed += active
            active = 0
        projected = mode == 'projected' or (mode in ('mixed', 'clamped') and local)
        sample = committed + (active if projected else 0)
        delta = sample - prev
        negative += delta < 0
        if mode == 'clamped':
            delta = max(0, delta)
        used = min(1, delta)
        prev += used
        charge = 1 - used
        clock += charge
        error += abs(charge - exact)
        steps.append((t, irq, local, sample, delta, charge, exact))
    return error, negative, clock, steps


def main():
    modes = ('upstream', 'projected', 'mixed', 'clamped', 'flush')
    totals = {mode: [0, 0, 0] for mode in modes}
    count = 0
    for contexts in product((0, 1), repeat=8):
        for readers in product((0, 1), repeat=8):
            count += 1
            for mode in modes:
                err, neg, _, _ = evaluate(contexts, readers, mode)
                totals[mode][0] += bool(err)
                totals[mode][1] += err
                totals[mode][2] += bool(neg)
    print('traces:', count)
    for mode in modes:
        print(mode, 'erroneous_traces,total_absolute_error,negative_delta_traces:', *totals[mode])
    assert totals['projected'] == [0, 0, 0]
    assert totals['mixed'][2] > 0
    assert totals['flush'][2] == 0
    assert totals['clamped'][0] > 0
    print('counterexample: two consecutive IRQ intervals, local then remote read')
    print('t irq local sample delta charge exact')
    for mode in modes:
        print(mode, evaluate((1, 1), (1, 0), mode)[3])


if __name__ == '__main__':
    main()
