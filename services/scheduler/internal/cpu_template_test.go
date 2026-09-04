package scheduler

import (
	"encoding/json"
	"fmt"
	"testing"
)

// bm32 / bm64 format a uint value as a Firecracker-style bitmap string.
func bm32(v uint32) string { return fmt.Sprintf("0b%032b", v) }
func bm64(v uint64) string { return fmt.Sprintf("0b%064b", v) }

func buildConfig(kvmCaps []string, cpuidEntries []cpuidModifier, msrEntries []msrModifier) string {
	if kvmCaps == nil {
		kvmCaps = []string{}
	}
	if cpuidEntries == nil {
		cpuidEntries = []cpuidModifier{}
	}
	if msrEntries == nil {
		msrEntries = []msrModifier{}
	}
	b, _ := json.Marshal(cpuConfig{
		KvmCapabilities: kvmCaps,
		CpuidModifiers:  cpuidEntries,
		MsrModifiers:    msrEntries,
	})
	return string(b)
}

func parsedResult(t *testing.T, jsons []string) cpuConfig {
	t.Helper()
	out, err := IntersectCpuConfigs(jsons)
	if err != nil {
		t.Fatalf("IntersectCpuConfigs error: %v", err)
	}
	var cfg cpuConfig
	if err := json.Unmarshal([]byte(out), &cfg); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	return cfg
}

func TestIntersectCpuConfigs_Empty(t *testing.T) {
	out, err := IntersectCpuConfigs(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "" {
		t.Errorf("want empty string, got %q", out)
	}
}

func TestIntersectCpuConfigs_Single(t *testing.T) {
	safe := cpuidModifier{Leaf: "0x1", Subleaf: "0x0", Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(1)}}}
	readOnly := []cpuidModifier{{Leaf: "0xb", Subleaf: "0x1", Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(2)}}}, {Leaf: "0x1f", Subleaf: "0x1", Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(2)}}}}
	input := buildConfig(nil, append([]cpuidModifier{safe}, readOnly...), nil)
	want := buildConfig(nil, []cpuidModifier{safe}, nil)
	out, err := IntersectCpuConfigs([]string{input})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != want {
		t.Errorf("single config: want %q, got %q", want, out)
	}
}

func TestIntersectCpuConfigs_CpuidBitmapAND(t *testing.T) {
	// Two configs differ only in the eax bitmap for leaf 0x1.
	// Result should be the bitwise AND.
	modA := cpuidModifier{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFFFFFFFF)}},
	}
	modB := cpuidModifier{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x0F0F0F0F)}},
	}
	cfgA := buildConfig(nil, []cpuidModifier{modA}, nil)
	cfgB := buildConfig(nil, []cpuidModifier{modB}, nil)

	result := parsedResult(t, []string{cfgA, cfgB})

	if len(result.CpuidModifiers) != 1 {
		t.Fatalf("want 1 cpuid entry, got %d", len(result.CpuidModifiers))
	}
	got := result.CpuidModifiers[0].Modifiers[0].Bitmap
	want := bm32(0x0F0F0F0F)
	if got != want {
		t.Errorf("eax bitmap: want %s, got %s", want, got)
	}
}

func TestIntersectCpuConfigs_CpuidExtraLeafDropped(t *testing.T) {
	// Config A has leaf 0x1 and 0x7; config B has only leaf 0x1.
	// Leaf 0x7 must be absent from the result.
	leaf1 := cpuidModifier{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFFFF)}},
	}
	leaf7 := cpuidModifier{
		Leaf: "0x7", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "ebx", Bitmap: bm32(0xABCD)}},
	}
	cfgA := buildConfig(nil, []cpuidModifier{leaf1, leaf7}, nil)
	cfgB := buildConfig(nil, []cpuidModifier{leaf1}, nil)

	result := parsedResult(t, []string{cfgA, cfgB})

	for _, m := range result.CpuidModifiers {
		if m.Leaf == "0x7" {
			t.Error("leaf 0x7 should have been dropped from intersection")
		}
	}
	if len(result.CpuidModifiers) != 1 {
		t.Errorf("want 1 cpuid entry, got %d", len(result.CpuidModifiers))
	}
}

func TestIntersectCpuConfigs_CpuidExtraRegisterDropped(t *testing.T) {
	// Config A has eax+ecx for leaf 0x1; config B has only eax.
	// ecx must be absent from the result.
	modA := cpuidModifier{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{
			{Register: "eax", Bitmap: bm32(0xF)},
			{Register: "ecx", Bitmap: bm32(0xF)},
		},
	}
	modB := cpuidModifier{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x3)}},
	}
	cfgA := buildConfig(nil, []cpuidModifier{modA}, nil)
	cfgB := buildConfig(nil, []cpuidModifier{modB}, nil)

	result := parsedResult(t, []string{cfgA, cfgB})

	if len(result.CpuidModifiers) != 1 {
		t.Fatalf("want 1 cpuid entry, got %d", len(result.CpuidModifiers))
	}
	regs := result.CpuidModifiers[0].Modifiers
	if len(regs) != 1 || regs[0].Register != "eax" {
		t.Errorf("want only eax register, got %+v", regs)
	}
	if regs[0].Bitmap != bm32(0x3) {
		t.Errorf("eax bitmap: want %s, got %s", bm32(0x3), regs[0].Bitmap)
	}
}

func TestIntersectCpuConfigs_MsrBitmapAND(t *testing.T) {
	msrA := msrModifier{Addr: "0x10a", Bitmap: bm64(0xFFFFFFFFFFFFFFFF)}
	msrB := msrModifier{Addr: "0x10a", Bitmap: bm64(0x00000000000000FF)}
	cfgA := buildConfig(nil, nil, []msrModifier{msrA})
	cfgB := buildConfig(nil, nil, []msrModifier{msrB})

	result := parsedResult(t, []string{cfgA, cfgB})

	if len(result.MsrModifiers) != 1 {
		t.Fatalf("want 1 msr entry, got %d", len(result.MsrModifiers))
	}
	got := result.MsrModifiers[0].Bitmap
	want := bm64(0x00000000000000FF)
	if got != want {
		t.Errorf("msr bitmap: want %s, got %s", want, got)
	}
}

func TestIntersectCpuConfigs_MsrExtraAddrDropped(t *testing.T) {
	msrCommon := msrModifier{Addr: "0x10a", Bitmap: bm64(0xFF)}
	msrExtra := msrModifier{Addr: "0x10b", Bitmap: bm64(0xFF)}
	cfgA := buildConfig(nil, nil, []msrModifier{msrCommon, msrExtra})
	cfgB := buildConfig(nil, nil, []msrModifier{msrCommon})

	result := parsedResult(t, []string{cfgA, cfgB})

	for _, m := range result.MsrModifiers {
		if m.Addr == "0x10b" {
			t.Error("msr 0x10b should have been dropped from intersection")
		}
	}
	if len(result.MsrModifiers) != 1 {
		t.Errorf("want 1 msr entry, got %d", len(result.MsrModifiers))
	}
}

func TestIntersectCpuConfigs_KvmCapabilities(t *testing.T) {
	cfgA := buildConfig([]string{"cap-a", "cap-b"}, nil, nil)
	cfgB := buildConfig([]string{"cap-b", "cap-c"}, nil, nil)

	result := parsedResult(t, []string{cfgA, cfgB})

	if len(result.KvmCapabilities) != 1 || result.KvmCapabilities[0] != "cap-b" {
		t.Errorf("kvm_capabilities: want [cap-b], got %v", result.KvmCapabilities)
	}
}

func TestIntersectCpuConfigs_ThreeConfigs(t *testing.T) {
	// Verifies AND is applied across more than two configs.
	mod := func(v uint32) cpuidModifier {
		return cpuidModifier{
			Leaf: "0x1", Subleaf: "0x0", Flags: 0,
			Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(v)}},
		}
	}
	cfgA := buildConfig(nil, []cpuidModifier{mod(0xFF)}, nil)
	cfgB := buildConfig(nil, []cpuidModifier{mod(0x0F)}, nil)
	cfgC := buildConfig(nil, []cpuidModifier{mod(0x03)}, nil)

	result := parsedResult(t, []string{cfgA, cfgB, cfgC})

	got := result.CpuidModifiers[0].Modifiers[0].Bitmap
	want := bm32(0x03)
	if got != want {
		t.Errorf("three-way AND: want %s, got %s", want, got)
	}
}

func TestIntersectCpuConfigs_HexNormalization(t *testing.T) {
	// "0x01" and "0x1" should identify the same leaf.
	modA := cpuidModifier{
		Leaf: "0x01", Subleaf: "0x00", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}
	modB := cpuidModifier{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x0F)}},
	}
	cfgA := buildConfig(nil, []cpuidModifier{modA}, nil)
	cfgB := buildConfig(nil, []cpuidModifier{modB}, nil)

	result := parsedResult(t, []string{cfgA, cfgB})

	if len(result.CpuidModifiers) != 1 {
		t.Fatalf("want 1 cpuid entry (hex-normalized), got %d", len(result.CpuidModifiers))
	}
	got := result.CpuidModifiers[0].Modifiers[0].Bitmap
	want := bm32(0x0F)
	if got != want {
		t.Errorf("hex normalization AND: want %s, got %s", want, got)
	}
}

func TestIntersectCpuConfigs_IdenticalConfigs(t *testing.T) {
	// Identical configs should produce the same bitmaps unchanged.
	mod := cpuidModifier{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xABCD1234)}},
	}
	cfg := buildConfig(nil, []cpuidModifier{mod}, nil)

	result := parsedResult(t, []string{cfg, cfg})

	got := result.CpuidModifiers[0].Modifiers[0].Bitmap
	want := bm32(0xABCD1234)
	if got != want {
		t.Errorf("identical AND: want %s, got %s", want, got)
	}
}

func TestIntersectCpuConfigs_InvalidJSON(t *testing.T) {
	_, err := IntersectCpuConfigs([]string{"{}", "not-json"})
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestIntersectCpuConfigs_InvalidBitmap(t *testing.T) {
	modBad := cpuidModifier{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: "0bXXXX"}},
	}
	bad := buildConfig(nil, []cpuidModifier{modBad}, nil)
	good := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)

	_, err := IntersectCpuConfigs([]string{good, bad})
	if err == nil {
		t.Error("expected error for invalid bitmap string")
	}
}

func TestIntersectCpuConfigs_EmptyBothSlices(t *testing.T) {
	// Ensures empty arrays serialize as [] not null.
	cfgA := buildConfig(nil, nil, nil)
	cfgB := buildConfig(nil, nil, nil)

	out, err := IntersectCpuConfigs([]string{cfgA, cfgB})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(out), &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, field := range []string{"kvm_capabilities", "cpuid_modifiers", "msr_modifiers"} {
		if string(raw[field]) == "null" {
			t.Errorf("%s serialized as null, want []", field)
		}
	}
}
