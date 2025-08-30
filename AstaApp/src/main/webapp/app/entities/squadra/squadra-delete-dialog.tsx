import React, { useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Button, Modal, ModalBody, ModalFooter, ModalHeader } from 'reactstrap';

import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';
import { deleteEntity, getEntity } from './squadra.reducer';

export const SquadraDeleteDialog = () => {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const { id } = useParams<'id'>();

  const [loadModal, setLoadModal] = useState(false);

  useEffect(() => {
    dispatch(getEntity(id));
    setLoadModal(true);
  }, []);

  const squadraEntity = useAppSelector(state => state.squadra.entity);
  const updateSuccess = useAppSelector(state => state.squadra.updateSuccess);

  const handleClose = () => {
    navigate('/squadra');
  };

  useEffect(() => {
    if (updateSuccess && loadModal) {
      handleClose();
      setLoadModal(false);
    }
  }, [updateSuccess]);

  const confirmDelete = () => {
    dispatch(deleteEntity(squadraEntity.id));
  };

  return (
    <Modal isOpen toggle={handleClose}>
      <ModalHeader toggle={handleClose} data-cy="squadraDeleteDialogHeading">
        Conferma operazione di cancellazione
      </ModalHeader>
      <ModalBody id="astaAppApp.squadra.delete.question">Sei sicuro di volere eliminare Squadra {squadraEntity.id}?</ModalBody>
      <ModalFooter>
        <Button color="secondary" onClick={handleClose}>
          <FontAwesomeIcon icon="ban" />
          &nbsp; Annulla
        </Button>
        <Button id="jhi-confirm-delete-squadra" data-cy="entityConfirmDeleteButton" color="danger" onClick={confirmDelete}>
          <FontAwesomeIcon icon="trash" />
          &nbsp; Elimina
        </Button>
      </ModalFooter>
    </Modal>
  );
};

export default SquadraDeleteDialog;
