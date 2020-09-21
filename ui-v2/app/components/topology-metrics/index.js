import Component from '@glimmer/component';
import { tracked } from '@glimmer/tracking';
import { action } from '@ember/object';
import { inject as service } from '@ember/service';

export default class ChartThingyComponent extends Component {
  // =services
  @service dom;

  // =attributes
  tagName = '';

  @tracked downDim;

  // =methods
  get downDimension() {
    return this.downDim;
  }
  getDimensions(item) {
    return this.dom.element(item).getBoundingClientRect();
  }

  // =actions
  @action
  resize() {
    const dimensions = this.getDimensions('#downstream-lines');
    this.downDim = dimensions;
    // console.log(this.dimensions);
  }
}
