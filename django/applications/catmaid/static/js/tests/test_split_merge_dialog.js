/* jshint esversion: 8 */

QUnit.test('Split preview matches the submitted annotations for either pane order', async function(assert) {
  const sandbox = sinon.sandbox.create();
  const originalProject = window.project;
  const target = 'experiment:target';
  const source = 'source-fragment:example';
  const ordinary = 'revision needed';
  const annotations = [target, source, ordinary].map(name => ({name: name, users: [{id: 3, name: 'Editor'}]}));

  window.project = {id: 1};
  sandbox.stub(CATMAID.Confirmation3dDialog.prototype, 'populate');
  sandbox.stub(CATMAID.Annotations, 'forSkeleton').returns(Promise.resolve(annotations));
  sandbox.stub(CATMAID.Skeletons, 'getSamplerCount').returns(Promise.resolve(0));
  sandbox.stub($.fn, 'dialog');

  try {
    for (const downstreamLength of [20, 80]) {
      const dialog = Object.create(CATMAID.SplitMergeDialog.prototype);
      dialog.dialog = document.createElement('div');
      dialog.controlPanel = document.createElement('div');
      dialog.customOptions = document.createElement('div');
      dialog.model1_id = 1;
      dialog.models = {1: {baseName: 'Original'}};
      dialog.splitNodeId = 4;
      dialog.in_merge_mode = false;
      const skeleton = {
        createArbor: () => ({
          edges: {4: 2},
          cableLength: () => 100,
          countNodes: () => 6,
          subArbor: () => ({cableLength: () => downstreamLength, countNodes: () => 3}),
        }),
        getPositions: () => ({}),
        getActorColorAsHTMLHex: () => 'ffff00',
      };
      dialog.webglapp = {
        options: {},
        addSkeletons: (models, draw) => draw(),
        space: {content: {skeletons: {1: skeleton}}},
        getSkeletonColor: () => ({r: 1, g: 1, b: 0}),
        render: () => {},
      };

      dialog.populate();
      await Promise.resolve();
      // Use the actual dialog subtree so map extraction sees the rendered checkboxes.
      dialog.dialog.appendChild(dialog.controlPanel);

      const downstreamIsLarger = downstreamLength > 50;
      const originalTag = downstreamIsLarger ? 'under' : 'over';
      const newTag = downstreamIsLarger ? 'over' : 'under';
      const pane = tag => dialog.controlPanel.querySelector('#split_merge_dialog_' + tag + '_annotations');
      const checkbox = (tag, name) => pane(tag).querySelector('input[annotation="' + name + '"]');
      assert.ok(pane(originalTag).previousSibling.textContent.includes('remaining skeleton'));
      assert.ok(pane(newTag).previousSibling.textContent.includes('new skeleton'));
      const paneColor = tag => pane(tag).parentNode.previousSibling.style.backgroundColor;
      assert.strictEqual(paneColor(originalTag), 'rgb(255, 255, 0)');
      assert.strictEqual(paneColor(newTag), 'rgb(140, 140, 0)');
      assert.notOk(checkbox(originalTag, target).disabled);
      assert.notOk(checkbox(newTag, target).disabled);
      assert.ok(checkbox(originalTag, source).checked && checkbox(newTag, source).checked);

      const toggleNew = pane(newTag).parentNode.previousSibling.querySelector('input');
      for (const checked of [true, false]) {
        toggleNew.checked = checked;
        toggleNew.onchange();
        assert.strictEqual(checkbox(newTag, source).checked, true);
        assert.strictEqual(checkbox(newTag, target).checked, checked);
      }
      checkbox(originalTag, target).checked = false;
      checkbox(newTag, target).checked = true;
      checkbox(originalTag, ordinary).checked = false;
      assert.notOk(dialog.check_split_annotations(), 'Editable annotations must stay on at least one piece');
      checkbox(newTag, ordinary).checked = true;
      assert.ok(dialog.confirm(), 'The new skeleton can keep all editable annotations');

      const submit = sinon.spy();
      dialog.split = submit;
      dialog.onOK();
      assert.deepEqual(submit.firstCall.args, [
        {[source]: 3},
        {[target]: 3, [source]: 3, [ordinary]: 3},
        2,
      ], 'Submit the selections and the parent shown in the preview');
    }
  } finally {
    sandbox.restore();
    window.project = originalProject;
  }
});
